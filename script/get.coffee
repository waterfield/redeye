msgpack = require 'msgpack'
redis = require 'redis'
argv = require('optimist').argv

port = argv.p ? 6379
host = argv.h ? 'localhost'
slice = argv.s ? process.env['SLICE'] ? 2

r = redis.createClient port, host, return_buffers: true
r.select slice

r.get process.argv[2], (err, buf) ->
  throw err if err
  console.log msgpack.unpack(buf)
  r.end()
