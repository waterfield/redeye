redis = require 'redis'
msgpack = require 'msgpack'

port = 6379
host = 'localhost'

usage = ->
  console.log "Usage: SLICE=2 coffee loose.coffee KEY"
  process.exit 1

usage() unless root = process.argv[2]

db = redis.createClient port, host, detect_buffers: true
db.select process.env['SLICE'] if process.env['SLICE']

loose = []

trace = (key, callback) ->
  db.get 'lock:'+key, (err, lock) ->
    return callback(err) if err
    if lock == 'ready'
      callback()
    else
      db.smembers 'sources:'+key, (err, sources) ->
        return callback(err) if err
        if sources.length
          next = (err) ->
            return callback(err) if err
            return callback() unless sources.length
            trace sources.shift(), next
          next()
        else
          loose.push key
          callback()

trace root, (err) ->
  throw err if err
  db.end()
  console.log { loose }
