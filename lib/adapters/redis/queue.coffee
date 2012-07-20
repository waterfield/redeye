RedisAdapter = require './adapter'

module.exports = class RedisQueue extends RedisAdapter
  push: (name, value, callback) ->
    @redis.rpush name, value, callback
  rpush: (name, value, callback) ->
    @redis.rpush name, value, callback
  lpush: (name, value, callback) ->
    @redis.lpush name, value, callback
  pop: (name, callback) ->
    @redis.blpop name, 0, (err, [k, v]) =>
      return callback(err) if err
      callback(null, v)
  del: (name, callback) ->
    @redis.del name, callback
  range: (name, from, to, callback) ->
    @redis.lrange name, from, to, callback
