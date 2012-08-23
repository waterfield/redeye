consts = require './consts'
db = require './db'
_ = require 'underscore'
require './util'

module.exports = class RequestChannel
  constructor: (options) ->
    {db_index} = options
    @_pubsub = db.pub_sub options
    @_channel = _('requests').namespace db_index

  end: -> @_pubsub.end()
  
  connect: (callback) ->
    @_pubsub.connect callback

  listen: (callback) ->
    @_pubsub.message (ch, str) ->
      [source, keys...] = str.split consts.key_sep
      callback source, keys
    @_pubsub.subscribe @_channel
