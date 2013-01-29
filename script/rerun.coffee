msgpack = require 'msgpack'
redis = require 'redis'
Manager = require '../lib/manager'
_ = require '../lib/util'

# Print usage message and die
usage = ->
  console.log "Usage: coffee script/rerun.coffee -w <worker> [-p <port>] [-s <slice>]"
  process.exit 1

# Parse argument options
argv = require('optimist').argv
slice = argv.s ? process.env['SLICE'] ? 2
port = argv.p ? process.env['REDIS_PORT'] ? 6379
seed = argv.seed ? process.env['SEED'] ? null

usage() unless argv.w

# Create redis connection
r = redis.createClient port, 'localhost', detect_buffers: true
r.select slice

# Globals
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
  unless seed
    console.log "Couldn't determine seed :("
    r.end()
    return
  console.log "Seed: #{seed}"
  manager = new Manager { slice }
  require(argv.w).init manager
  manager.run()
  manager.on 'ready', ->
    r.publish "requests_#{slice}", '!reset'
    r.end()
    setTimeout listen_for_completion, 500
  manager.on 'quit', ->
    console.log 'Done'
    r.end()

listen_for_completion = ->
  manager.request seed
  r = redis.createClient port, 'localhost', return_buffers: true
  r.select slice
  r.subscribe 'redeye:finish'
  r.on 'message', (c, msg) ->
    manager.quit() if msgpack.unpack(msg).key == seed
  # manager.on 'redeye:finish', (log) ->
  #   manager.quit() if log.key == seed

# Delete all the collected intermediate keys, then call `rerun`.
delete_keys = ->
  if to_delete.length
    chunks = _(to_delete).in_groups_of(10000)
    each chunks, rerun, (chunk, next) ->
      r.del chunk..., next
  else
    console.log "No keys to delete"
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
            if sources || (prefix == 'one_shot_cashout')
              seed ?= key unless targets
              to_delete.push key
              to_delete.push 'lock:'+key
              to_delete.push 'sources:'+key
              to_delete.push 'targets:'+key
            next()

scan_db()
