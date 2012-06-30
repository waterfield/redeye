RedisAdapter = require './adapter'

module.exports = class RedisKeyValue extends RedisAdapter
  get: (key, callback) ->
    @redis.get key, callback
  get_all: (keys, callback) ->
    @redis.mget keys, callback
  keys: (pattern, callback) ->
    @redis.keys pattern, callback
  set: (key, value, callback) ->
    @redis.set key, value, callback
  atomic_set: (key, value, callback) ->
    @redis.setnx key, value, (err) =>
      return callback(err) if err
      @redis.get key, callback
  map_reduce: (pattern, map, reduce, callback) ->
    @keys pattern, (err, keys) =>
      return callback(err) if err
      @get_all keys, (err, values) =>
        try
          return callback(err) if err
          results = {}
          emit = (key, value) ->
            (results[key] ?= []).push value
          map(value, emit) for value in values
          for key, values of results
            results[key] = reduce key, values
            callback null, results
        catch e
          callback e
  del: (key, callback) ->
    @redis.del key, callback
  flush: (callback) ->
    @redis.flushdb callback
