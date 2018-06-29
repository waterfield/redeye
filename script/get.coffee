msgpack = require 'msgpack'
redis = require 'redis'
_ = require '../lib/util'

opts = require('optimist')
  .usage('Print packed values from redis.\n\nUsage: $0 "key or pattern"')
  .boolean('n').alias('n', 'no-nulls').describe('n', 'ignore null and empty values')
  .alias('p', 'port').default('p', 6379).describe('p', 'redis port')
  .alias('h', 'host').default('h', 'localhost').describe('h', 'redis host')
  .alias('s', 'slice').default('s', 2).describe('s', 'redis slice')
  .alias('w', 'workers').describe('w', 'location of worker file(s)')
  .boolean('help').describe('help', 'print this help')
  .string('e').alias('e', 'equal-to').describe('e', 'only select matching values')
  .string('x').alias('x', 'not-equal-to').describe('x', 'only select NON-matching values')
  .demand(1)

{ argv } = opts
if argv.help
  opts.showHelp()
  process.exit 0
port = argv.p
host = argv.h
slice = argv.s ? process.env['SLICE'] ? 2

packs = {}

ne = JSON.parse(argv.x) if argv.x?
eq = JSON.parse(argv.e) if argv.e?

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
  return array unless array
  if _.isArray array[0]
    unpack_fields(elem, fields) for elem in array
  else
    hash = {}
    for field, index in fields
      hash[field] = array[index]
    hash

r = redis.createClient port, host, return_buffers: true
r.select slice

present = (value) ->
  value && (!_.isArray(value) || value.length)

accept = (value) ->
  if present value
    if ne?
      !_.isEqual(ne, value)
    else if eq?
      _.isEqual(eq, value)
    else
      true
  else
    !argv.n

get_keys = (pattern, callback) ->
  r.keys pattern, (err, keys) ->
    return callback(err) if err
    keys = (key for key in keys when !(/^(sources|lock|targets):/.test key))
    callback null, keys

get = (key, callback) ->
  prefix = key.split(':')[0]
  # console.log { prefix, key }
  r.get key, (err, buf) ->
    return callback(err) if err
    if buf
      # console.log {buf}
      value = JSON.parse buf
      # console.log {value}
      if value && (fields = packs[prefix])?
        value = unpack_fields value, fields
      if accept value
        console.log "\n-- #{key} --\n"
        console.log value
        sources key, callback
      else
        callback()
    else if !argv.n
      console.log "\n-- #{key} --\n"
      console.log '<missing>'
      callback()
    else
      callback()

sources = (key, callback) ->
  r.smembers "sources:#{key}", (err, arr) ->
    return callback(err) if err
    if arr.length
      console.log "\n  Sources:"
      for key in arr
        console.log "    #{key}"
    else
      console.log "\n  No sources."
    callback()

get_all = (pattern, callback) ->
  get_keys pattern, (err, keys) ->
    return callback(err) if err
    if keys.length
      console.log "#{keys.length} keys matched this pattern."
      next = ->
        if key = keys.shift()
          get key.toString(), next
        else
          callback()
      next()
    else
      console.log 'No keys matched this pattern.'
      callback()

finalize = (err) ->
  r.end()
  throw err if err

if argv._[0].indexOf('*') >= 0
  get_all argv._[0], finalize
else
  get argv._[0], finalize
