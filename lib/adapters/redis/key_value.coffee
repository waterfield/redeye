RedisAdapter = require './adapter'

module.exports = class RedisKeyValue extends RedisAdapter
  get: (key, callback) ->
    log = key.split(':')[0] == 'ca_gas'
    console.log 'get', key if log
    @redis.get key, (err, val) ->
      console.log 'get done', key if log
      callback err, (JSON.parse(val) if val)
  get_all: (keys, callback) ->
    @redis.mget keys, (err, arr) ->
      return callback(err) if err
      callback null, (JSON.parse(val) for val in arr)
  keys: (pattern, callback) ->
    @redis.keys pattern, callback
  set: (key, value, callback) ->
    log = key.split(':')[0] == 'ca_gas'
    console.log 'set', key if log
    @redis.set key, JSON.stringify(value), (err) ->
      console.log 'set done', key if log
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
