RedisAdapter = require './adapter'

module.exports = class RedisKeyValue extends RedisAdapter
  get: (key, callback) ->
    @redis.get key, (err, val) ->
      try
        callback err, (JSON.parse(val) if val)
      catch e
        console.log 'failed trying to parse key', key, ':', val
        throw e
  get_all: (keys, callback) ->
    @redis.mget keys, (err, arr) ->
      return callback(err) if err
      callback null, (JSON.parse(val) for val in arr)
  keys: (pattern, callback) ->
    @redis.keys pattern, callback
  set: (key, value, callback) ->
    @redis.set key, JSON.stringify(value), (err) ->
      callback err if callback
  exists: (key, callback) ->
    @redis.exists key, callback
  atomic_set: (key, value, callback) ->
    @redis.setnx key, JSON.stringify(value), (err) =>
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
