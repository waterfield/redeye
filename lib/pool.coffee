redis = require 'redis'
{ Pool } = require 'generic-pool'

port = 6379
host = '127.0.0.1'
opts = detect_buffers: true

module.exports = ->
  Pool
    max: 1000
    # log: true
    create: (callback) ->
      client = redis.createClient port, host, opts
      callback null, client
    destroy: (client) ->
      client.end()
