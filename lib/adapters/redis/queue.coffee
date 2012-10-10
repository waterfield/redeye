RedisAdapter = require './adapter'

module.exports = class RedisQueue extends RedisAdapter
  push: (name, value, callback) ->
    @redis.rpush name, value, callback
  rpush: (name, value, callback) ->
    @redis.rpush name, value, callback
  lpush: (name, value, callback) ->
    @redis.lpush name, value, callback
  push_all: (name, values, callback) ->
    @redis.rpush name, values..., callback
  rpush_all: (name, values, callback) ->
    @redis.rpush name, values..., callback
  lpush_all: (name, values, callback) ->
    @redis.lpush name, values..., callback
  pop: (name, callback) ->
    @redis.blpop name, 0, (err, value) =>
      return callback(err) if err
      callback(null, value[1])
  pop_any: (names..., callback) ->
    @redis.blpop names..., 0, callback
  smbembers: (name, callback) ->
    @redis.smembers name, callback
  del: (name, callback) ->
    @redis.del name, callback
  range: (name, from, to, callback) ->
    @redis.lrange name, from, to, (err, arr) ->
      return callback(err) if err
      arr = (JSON.parse(str) for str in arr)
      callback null, arr
  watch: (name, callback, root) ->
    @pop name, (err, value) ->
      return callback.call root, err if err
      process.nextTick =>
        @watch name, callback, root
      callback.call root, null, value
