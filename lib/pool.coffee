redis = require 'redis'
{ Pool } = require 'generic-pool'

port = 6379
host = '127.0.0.1'

module.exports = (opts) ->
  Pool
    max: 1000
    # log: true
    create: (callback) ->
      client = redis.createClient port, host, detect_buffers: true
      client.select(opts.slice) if opts.slice
      callback null, client
    destroy: (client) ->
      client.end()
