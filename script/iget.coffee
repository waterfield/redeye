
redis = require 'redis'

r = redis.createClient 6379, 'localhost', return_buffers: true

global.select = (slice) -> r.select slice

global.get = (args...) ->
  key = args.join ':'
  r.get key, (err, buf) ->
    throw err if err
    global.data = global[args[0]] = JSON.parse buf
