msgpack = require 'msgpack'
redis = require 'redis'

global.data = {}
r = redis.createClient 6379, 'localhost', return_buffers: true

global.get = (key, slice=global.slice) ->
  r.select slice if slice?

  r.get key, (err, buf) ->
    throw err if err
    global.data = msgpack.unpack(buf)
