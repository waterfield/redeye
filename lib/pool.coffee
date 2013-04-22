redis = require 'redis'
stats = require('./stats').getChildClient('pool.redis')
{ Pool } = require 'generic-pool'

default_port = 6379
default_host = '127.0.0.1'

pool = null
interval = null

gauge = ->
  stats.gauge 'size', pool.getPoolSize()
  stats.gauge 'available', pool.availableObjectsCount()
  stats.gauge 'waiting', pool.waitingClientsCount()

module.exports = (opts) ->
  port = opts.port ? default_port
  host = opts.host ? default_host
  pool = Pool
    max: 1000
    # log: true
    create: (callback) ->
      client = redis.createClient port, host, detect_buffers: true
      client.select(opts.slice) if opts.slice
      callback null, client
    destroy: (client) ->
      client.end()
  pool.quit = (callback) ->
    pool.drain ->
      pool.destroyAllNow ->
        clearInterval interval
        callback?()
  interval = setInterval gauge, 1000
  pool
