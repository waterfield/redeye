consts = require './consts'
db = require './db'
_ = require 'underscore'
require './util'

module.exports = class ResponseChannel
  constructor: (@_pubsub, options) ->
    {db_index} = options
    @_channel = _('responses').namespace db_index

  end: -> @_pubsub.end()

  listen: (callback) ->
    @_pubsub.message callback
    @_pubsub.subscribe @_channel
