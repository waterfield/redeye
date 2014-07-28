_ = require '../lib/util'
msgpack = require 'msgpack'
argv = require('optimist').argv

class Index

  constructor: ->
    @workers = []

  namespace: (namespace, body) ->
    @default_namespace = namespace
    body()
    @default_namespace = undefined

  input: (args...) ->
    @worker args..., null

  worker: (prefix, params..., body) ->
    opts = _.opts params
    namespace = opts.namespace ? @default_namespace
    prefix = "#{namespace}.#{prefix}" if namespace
    @workers.push { prefix, params }

  mixin: ->

  dump: ->
    console.log @workers

index = new Index
require(argv._[0]).init index
index.dump()
