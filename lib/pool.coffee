redis = require 'redis'
{ Pool } = require 'generic-pool'

default_port = 6379
default_host = '127.0.0.1'

module.exports = (opts) ->
  port = opts.port ? default_port
  host = opts.host ? default_host
  Pool
    max: 100
    # log: true
    create: (callback) ->
      client = redis.createClient port, host, detect_buffers: true
      client.select(opts.slice) if opts.slice
      callback null, client
    destroy: (client) ->
      client.end()
