KeyValue = require '../../db/key_value'

class MongoKeyValue extends KeyValue
  get: (key, callback) ->
  get_all: (keys, callback) ->
  keys: (pattern, callback) ->
  set: (key, value, callback) ->
  atomic_set: (key, value, callback) ->
  del: (key, callback) ->
  flush: (callback) ->
  end: ->
  