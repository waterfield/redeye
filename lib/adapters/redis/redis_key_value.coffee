KeyValue = require '../../db/key_value'

class RedisKeyValue extends KeyValue
  constructor: (@db) ->
  get: (key, callback) ->
    @db.get key, (err, value) =>
      if err then @err(err) else callback(value)
  get_all: (keys, callback) ->
    @db.mget keys, (err, values) =>
      if err then @err(err) else callback(values)
  keys: (pattern, callback) ->
    @db.keys pattern, (err, keys) =>
      if err then @err(err) else callback(keys)
  set: (key, value, callback) ->
    @db.set key, value, (err) =>
      if err then @err(err) else callback?()
  atomic_set: (key, value, callback) ->
    @db.setnx key, value, (err) =>
      return @err(err) if err
      @db.get key, (err, value) =>
        if err then @err(err) else callback(value)
  del: (key, callback) ->
    @db.del key, (err) =>
      if err then @err(err) else callback?()
  flush: (callback) ->
    @db.flushdb (err) =>
      if err then @err(err) else callback?()
  end: ->
    @db.end()
  