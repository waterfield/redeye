msgpack = require 'msgpack'
redis = require 'redis'

r = redis.createClient 6379, 'localhost', return_buffers: true
r.select process.env['SLICE'] if process.env['SLICE']

r.get process.argv[2], (err, buf) ->
  throw err if err
  console.log JSON.stringify(msgpack.unpack(buf), null, 2)
  r.end()
