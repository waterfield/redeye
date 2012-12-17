msgpack = require 'msgpack'
redis = require 'redis'

r = redis.createClient 6379, 'localhost', return_buffers: true

r.get process.argv[2], (err, buf) ->
  throw err if err
  console.log msgpack.unpack(buf)
  r.end()
