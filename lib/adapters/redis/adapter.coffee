conn = require './connection'
_ = require 'underscore'

serializer = 'json'
# serializer = 'msgpack'

no_ser = -> throw new Error "Missing or invalid serializer"

if serializer == 'msgpack'
  {pack, unpack} = require 'msgpack'
else if serializer == 'json'
  pack = JSON.stringify
  unpack = JSON.parse
else
  pack = no_ser
  unpack = no_ser

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

_.extend RedisAdapter, {pack, unpack}
