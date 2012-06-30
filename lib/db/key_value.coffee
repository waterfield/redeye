Adapter = require './adapter'

class KeyValue extends Adapter
  get: (key, callback) ->
  get_all: (keys, callback) ->
  keys: (pattern, callback) ->
  set: (key, value, callback) ->
  atomic_set: (key, value, callback) ->
  del: (key, callback) ->
  flush: (callback) ->
  end: ->
