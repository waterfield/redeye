RedisAdapter = require './adapter'

module.exports = class RedisKeyValue extends RedisAdapter
  get: (key, callback) ->
    @redis.get key, (err, val) ->
      parsed = try
        RedisAdapter.unpack val
      catch e
        console.log 'failed trying to unpack key', key, ':', val, 'got error', e # XXX
        throw e
      callback err, parsed
  get_all: (keys, callback) ->
    @redis.mget keys, (err, arr) ->
      return callback(err) if err
      callback null, (RedisAdapter.unpack(val) for val in arr)
  keys: (pattern, callback) ->
    @redis.keys pattern, callback
  set: (key, value, callback) ->
    @redis.set key, RedisAdapter.pack(value), (err) ->
      callback err if callback
  exists: (key, callback) ->
    @redis.exists key, callback
  atomic_set: (key, value, callback) ->
    @redis.setnx key, RedisAdapter.pack(value), (err) =>
      return callback(err) if err
      @get key, callback
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
