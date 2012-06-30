RedisAdapter = require './adapater'

module.exports = class RedisPubSub extends RedisAdapter
  subscribe: (channel) ->
    @redis.subscribe channel
  publish: (channel, message) ->
    @redis.publish channel, message
  message: (callback) ->
    @redis.on 'message', callback
