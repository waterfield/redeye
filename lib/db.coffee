# class KeyValue
#   get: (key, callback) ->
#   get_all: (keys, callback) ->
#   keys: (pattern, callback) ->
#   set: (key, value, callback) ->
#   atomic_set: (key, value, callback) ->
#   del: (key, callback) ->
#   flush: (callback) ->
#   exists: (key, callback) ->
#   # map: (object, emit) -> ... emit(key, value) ...
#   # reduce: (key, values) -> ... return value
#   map_reduce: (pattern, map, reduce, callback) ->
#   end: ->

# class PubSub
#   subscribe: (channel) ->
#   publish: (channel, message) ->
#   message: (callback) ->
#   end: ->

# class Queue
#   push: (name, value) ->
#   pop: (name, callback) ->
#   del: (name) ->
#   end: ->

config = require './config'
for type, adapter of config.adapters
  do (type, adapter) ->
    klass = require "./adapters/#{adapter}/#{type}"
    exports[type] = (options) -> new klass(options ? {})
