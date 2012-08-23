PubSub = require '../../db/pub_sub'

class ZeromqPubSub extends PubSub
  subscribe: (channel) ->
  publish: (channel, message) ->
  message: (callback) ->
  end: ->
  