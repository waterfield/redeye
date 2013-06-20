msgpack = require 'msgpack'
redis = require 'redis'
argv = require('optimist').argv
_ = require '../lib/util'

port = argv.p ? 6379
host = argv.h ? 'localhost'
slice = argv.s ? process.env['SLICE'] ? 2

packs = {}

class FakeManager
  namespace: (namespace, body) ->
    @default_namespace = namespace
    body()
    @default_namespace = undefined
  input: (args...) ->
    @worker args..., null
  mixin: ->
  worker: (prefix, params..., body) ->
    opts = _.opts params
    namespace = opts.namespace ? @default_namespace
    prefix = "#{namespace}.#{prefix}" if namespace
    packs[prefix] = opts.pack if opts.pack?

manager = new FakeManager
require(argv.w).init(manager) if argv.w?

unpack_fields = (array, fields) ->
  if _.isArray array[0]
    unpack_fields(elem, fields) for elem in array
  else
    hash = {}
    for field, index in fields
      hash[field] = array[index]
    hash

r = redis.createClient port, host, return_buffers: true
r.select slice

get = (key) ->
  prefix = key.split(':')[0]

  r.get key, (err, buf) ->
    throw err if err
    if buf
      value = msgpack.unpack buf
      if value && (fields = packs[prefix])?
        value = unpack_fields value, fields
      console.log value
      sources key
    else
      console.log '<missing>'

sources = (key) ->
  r.smembers "sources:#{key}", (err, arr) ->
    console.log "\nSources:"
    for key in arr
      console.log "  #{key}"
    r.end()

get argv._[0]
