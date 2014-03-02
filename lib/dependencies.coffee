_ = require './util'
{EventEmitter2} = require 'eventemitter2'

class Dependencies extends EventEmitter2
  constructor: ->
    @values = {}
    @sources = {}
    @active = {}

  cycle: (source, target) ->
    return null unless @active[source]
    cycle = []
    key = source
    while key != target
      cycle.push key
      keys = @sources[key]
      key = keys[keys.length - 1]
    cycle

  require: (sources, target) ->
    arr = (@sources[target] ||= {})
    for source in sources
      arr.push source
      if source of @values
        @values[source]
      else
        @active[source] = true
        @send_job source
        undefined

  finish: (key, value) ->
    delete @active[key]
    @values[key] = value
    process.nextTick =>
      @emit 'ready', key

  send_job: (key) ->
    process.nextTick =>
      @emit 'job', key

  keys: (pattern) ->
    re = new RegExp('^' + _.gsub(pattern, '*', '.*') + '$')
    _.filter _.keys(@values), (key) -> re.test(key)

  get: (key) ->
    return @values[key]

  set: (key, value) ->
    @values[key] = value

  exists: (key) ->
    key of @values

  mget: (keys) ->
    @values[key] for key in keys
