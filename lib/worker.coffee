consts = require './consts'
db = require './db'
_ = require 'underscore'
require './util'
require 'fibers'

# Counts the number of simultaneous workers.
num_workers = 0

# The worker class is the context under which runner functions are run.
class Worker

  # Find the runner for the `@key`. The key is in the format:
  # 
  #     prefix:arg1:arg2:...
  constructor: (@key, @queue, @sticky) ->
    [@prefix, args...] = @key.split consts.arg_sep
    @args = args # weird bug in coffeescript: wanted @args... in line above
    @db = @queue.worker_db
    @req_channel = _('requests').namespace @queue.options.db_index
    @resp_channel = _('responses').namespace @queue.options.db_index
    @cache = {}
    unless @runner = @queue.runners[@prefix]
      @emit @key, null
      console.log "no runner for '#{@prefix}' (#{@key})"
      throw 'no_runner'
    num_workers++
  
  # If we've already seen this `@get` before, then return the actual
  # value we've received (which we know we got because otherwise we
  # wouldn't be running again). Otherwise, just mark this dependency
  # and return `undefined`.
  get: (args...) ->
    @on_cycle = _.callback args
    opts = _.opts args
    key = args.join consts.arg_sep
    @notify_dep key
    if saved = @sticky[key] ? @cache[key]
      return saved
    @db.get key, (err, val) =>
      if val
        @fiber.run val
      else
        @request_key key
    val = @yield()
    if @cycling
      @cycling = false
      val = @on_cycle.apply this
    else
      val = @build JSON.parse(val), opts.as
    @cache[key] = val
    @sticky[key] = val if opts.sticky
    val

  # Notify the dispatcher of our dependency (regardless of whether we're
  # going to request that key).
  notify_dep: (key) ->
    msg = ['!dep', @key, key].join consts.key_sep
    @db.publish @req_channel, msg
  
  # Search for the given keys in the database, then remember them.
  keys: (str) ->
    @db.keys str, (err, arr) =>
      @fiber.run arr
    @yield()
  
  yield: ->
    _.tap yield(), => Worker.current = this
  
  # This is a bit of syntactic sugar. It's the equivalent of:
  # 
  #     x = @get key
  #     @for_reals()
  #     x
  get_now: -> @get.apply this, arguments

  # If a klass is given, construct a new one; otherwise, just return
  # the raw value.
  build: (value, klass) ->
    klass ?= @wrapper_class
    if klass? then @bless(new klass(value)) else value
  
  # Extend the given object with the context methods of a worker,
  # in addition to a recursive blessing.
  bless: (object) ->
    for method in ['get', 'emit', 'for_reals', 'get_now', 'keys', 'worker']
      do (method) -> object[method] = ->
        Worker.current[method].apply Worker.current, arguments
    for method, fun of Worker.mixins
      do (method, fun) -> object[method] = ->
        fun.apply Worker.current, arguments
    object.bless = (next) => @bless next
    object

  # Produce `value` as a result for `key`. This both puts the result
  # in redis under the key and tells the dispatcher (via the `responses`
  # channel) that the job is done.
  emit: (args..., value) ->
    @emitted = true
    key = args.join consts.arg_sep
    json = value?.toJSON?() ? value
    @db.set key, JSON.stringify(json)
    @db.publish @resp_channel, key

  # If we've seen this `@for_reals` before, then blow right past it.
  # Otherwise, abort the runner function and start over (after checking
  # that our dependencies are met).
  for_reals: -> true

  # Attempt to run the runner function. If a call to `@for_reals` causes
  # us to abort, then attempt to resolve the dependencies.
  run: ->
    @fiber = Fiber =>
      @clear()
      @process()
    @fiber.run()

  # Reset information about this run, including:
  # 
  # * `@emitted`: whether `@emit` has been called.
  clear: ->
    @emitted = false
    Worker.clear_callback?.apply this
    
  # Mark that a fatal exception occurred
  error: (err) ->
    message = err.stack ? err
    console.log message
    @db.set 'fatal', message

  # Call the runner. If it gets all the way through, first check if there
  # were any unmet dependencies after the last `@for_reals`. If so, we force
  # one last dependency resolution. Otherwise, we optionally
  # emit the result of the function (if nothing has been emitted yet).
  process: ->
    Worker.current = this
    result = @runner.apply(this, @args)
    @finish result unless @_async
  
  async: (fun) ->
    fun.apply this
    yield().apply this
  
  sync: (fun) ->
    @fiber.run fun
  
  # We're done!
  finish: (result) ->
    Worker.finish_callback?.apply this
    num_workers--
    @queue.finish @key
    @emit @key, (result ? null) unless @emitted

  # Ask the dispatcher to providethe given keys by publishing on the
  # `requests` channel. Then block-wait to be signalled by a response
  # on a resume key. Once we get that response, try again to fetch the
  # dependencies (which should all be present).
  request_key: (key) ->
    @requested = key
    @db.publish @req_channel, "#{@key}#{consts.key_sep}#{key}"

  # The dispatcher said to resume, so go look for the missing values again. If
  # we're resuming from a cycle failure, go grab the key.
  resume: ->
    @db.get @requested, (err, val) =>
      @fiber.run val
  
  cycle: ->
    if @on_cycle
      @cycling = true
      @fiber.run()
  
  # Set the default wrapper class, which is overridden by `as: `
  wrapper: (klass) ->
    @wrapper_class = klass
  
  # Return the current worker
  worker: ->
    Worker.current


# Extend the blessed methods with the given ones, so that
# worker contexts can use them.
Worker.mixin = (mixins) ->
  _.extend (Worker.mixins ?= {}), mixins
  _.extend Worker.prototype, mixins

module.exports = Worker
