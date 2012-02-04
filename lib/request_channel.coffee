consts = require './consts'
db = require './db'
_ = require 'underscore'
require './util'

module.exports = class RequestChannel
  constructor: (options) ->
    {db_index} = options
    @_db = db db_index
    @_channel = _('requests').namespace db_index

  end: -> @_db.end()

  listen: (callback) ->
    @_db.on 'message', (ch, str) ->
      [source, keys...] = str.split consts.key_sep
      callback source, keys
    @_db.subscribe @_channel
