conn = require './connection'

serializer = 'json'
# serializer = 'msgpack'

if serializer == 'msgpack'
  msgpack = require 'msgpack'

module.exports = class RedisAdapter
  constructor: (options = {}) ->
    @redis = options.connection ? conn(options)
    @redis._uses ?= 0
    @redis._uses++
  connect: (callback) ->
    callback() if callback
  end: ->
    unless --@redis._uses
      @redis.end()

  if serializer == 'json'
    pack: JSON.stringify
    unpack: JSON.parse

  else if serializer == 'msgpack'
    pack: msgpack.pack
    unpack: msgpack.unpack
