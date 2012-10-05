RedisAdapter = require './adapter'

module.exports = class RedisQueue extends RedisAdapter
  push: (name, value, callback) ->
    @redis.rpush name, value, callback
  rpush: (name, value, callback) ->
    @redis.rpush name, value, callback
  lpush: (name, value, callback) ->
    @redis.lpush name, value, callback
  pop: (name, callback) ->
    @redis.blpop name, 0, (err, value) =>
      return callback(err) if err
      callback(null, value[1])
  del: (name, callback) ->
    @redis.del name, callback
  range: (name, from, to, callback) ->
    @redis.lrange name, from, to, (err, arr) ->
      return callback(err) if err
      arr = (JSON.parse(str) for str in arr)
      callback null, arr
