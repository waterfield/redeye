# class KeyValue
#   get: (key, callback) ->
#   get_all: (keys, callback) ->
#   keys: (pattern, callback) ->
#   set: (key, value, callback) ->
#   atomic_set: (key, value, callback) ->
#   del: (key, callback) ->
#   flush: (callback) ->
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

key_value = require "./adapters/#{config.adapters.key_value}/key_value"
pub_sub = require "./adapters/#{config.adapters.pub_sub}/pub_sub"
queue = require "./adapters/#{config.adapters.queue}/queue"

module.exports = {key_value, pub_sub, queue}
