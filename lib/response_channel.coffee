consts = require './consts'
db = require './db'
_ = require 'underscore'
require './util'

module.exports = class ResponseChannel
  constructor: (options) ->
    {db_index} = options
    @_db = db db_index
    @_channel = _('responses').namespace db_index

  end: -> @_db.end()

  listen: (callback) ->
    @_db.on 'message', callback
    @_db.subscribe @_channel
