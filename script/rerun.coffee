msgpack = require 'msgpack'
redis = require 'redis'

# Print usage message and die
usage = ->
  console.log "Usage: coffee script/rerun.coffee -w <worker> [-p <port>] [-s <slice>]"
  process.exit 1

# Parse argument options
argv = require('optimist').argv
slice = argv.s ? process.env['SLICE'] ? 2
port = argv.p ? process.env['REDIS_PORT'] ? 6379
usage() unless argv.w

# Create redis connection
r = redis.createClient 6379, 'localhost', detect_buffers: true
r.select slice

# Globals
seed = null
to_delete = []
manager = null

# Iterator (sequential mode)
each = (list, final, fun) ->
  index = 0
  next = ->
    return final() if ++index == list.length
    fun list[index], next
  next()

# Boot up manager with worker definitions and re-request
# seed key. Shut the script down when the seed key is done.
rerun = ->
  manager = new Manager { slice }
  require(argv.w).init manager
  manager.run()
  manager.on 'ready', ->
    manager.request seed
  manager.on 'quit', ->
    console.log 'Done'
    r.end()
  manager.on 'redeye:finish', (payload) ->
    { key } = payload
    manager.quit() if key == seed

# Delete all the collected intermediate keys, then call `rerun`.
delete_keys = ->
  r.del to_delete..., (err) ->
    throw err if err
    console.log "Deleted #{to_delete.length/4} keys."
    console.log "Seed: #{seed}"
    rerun()

# Find out what keys are intermediate and what is the seed,
# then call `delete_keys`.
scan_db = ->
  r.keys 'lock:*', (err, locks) ->
    throw err if err
    each locks, delete_keys, (lock, next) ->
      [unused, prefix, rest...] = lock.split ':'
      key = [prefix, rest...].join ':'
      do (key, prefix) ->
        r.scard "targets:#{key}", (err, targets) ->
          throw err if err
          r.scard "sources:#{key}", (err, sources) ->
            throw err if err
            if sources
              seed = key unless targets
              to_delete.push key
              to_delete.push 'lock:'+key
              to_delete.push 'sources:'+key
              to_delete.push 'targets:'+key
            next()

scan_db()
