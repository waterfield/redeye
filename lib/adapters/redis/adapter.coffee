conn = require './connection'

module.exports = class RedisAdapter
  constructor: (options) ->
    @redis = options.connection ? @_make_connection(options)
    @redis._uses ?= 0
    @redis._uses++
  end: ->
    unless --@redis._uses
      @redis.end()
  _make_connection: (options) ->
    conn options