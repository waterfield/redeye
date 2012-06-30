PubSub = require '../../db/pub_sub'

class RedisPubSub extends PubSub
  constructor: (@db) ->
  subscribe: (channel) ->
    @db.subscribe channel
  publish: (channel, message) ->
    @db.publish channel, message
  message: (callback) ->
    @db.on 'message', callback
  end: ->
    @db.end()
