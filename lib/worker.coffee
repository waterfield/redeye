consts = require './consts'
db = require './db'
_ = require 'underscore'
require './util'
CycleError = require './cycle_error'

# Counts the number of simultaneous workers.
num_workers = 0

# The worker class is the context under which runner functions are run.
# 
# FIXME: @keys and @get_now don't work with @async !!
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
    unless @runner = @queue.runners[@prefix]
      @emit @key, null
      console.log "no runner for '#{@prefix}' (#{@key})"
      throw 'no_runner'
    @cache = {}
    @saved_keys = {}
    @cycle = {}
    @tested_raise = {}
    @failed_raise = {}
    @sequence = []
    @last_stage = 0
    num_workers++
  
  # Mark the worker as asynchronous. If a callback is provided, it's
  # run in the context of the worker.
  async: (callback) ->
    @is_async = true
    callback.apply this if typeof(callback) == 'function'
  
  # Print a debugging statement
  debug: (args...) ->
    #console.log 'worker:', args...

  # If we've already seen this `@get` before, then return the actual
  # value we've received (which we know we got because otherwise we
  # wouldn't be running again). Otherwise, just mark this dependency
  # and return `undefined`.
  get: (args...) ->
    opts = _(args).opts()
    key = args.join consts.arg_sep
    #@check_stage key
    if @sticky[key]
      @sticky[key]
    else if @should_raise_cycle(key)
      @tested_raise[key] = true
      throw new CycleError key
    else if @stage < @last_stage
      value = @build @cache[key], opts.as
      @sticky[key] = value if opts.sticky
      value
    else
      @deps.push key
      @blank()
  
  # We should throw a CycleError for the given key if and only if this key
  #   A) caused a cycle at one point, and
  #   B) has NOT successfully handled this raise before
  should_raise_cycle: (key) ->
    @cycle[key] && (!@tested_raise[key] || !@failed_raise[key])
  
  # Return an instance of the default, not-yet-instantiated object. If
  # `@wrapper` was called, then it's an instance of this clas with `undefined`
  # as its value. Otherwise, an unwrapped `undefined` is returned.
  blank: ->
    if @wrapper_class?
      new @wrapper_class(undefined)
    else
      undefined
  
  # Make sure the given key is being requested in a totally legal and consistent way.
  check_stage: (key) ->
    if @key_index == @sequence.length
      @sequence.push key
    else
      if @sequence[@key_index] != key
        throw "#{@key} has a nondeterministic key sequence; expected #{@sequence[@key_index]}, but got #{key} (sequence: #{JSON.stringify(@sequence)})"
    @key_index++
  
  # Search for the given keys in the database, then remember them.
  keys: (str) ->
    if @saved_keys[str]
      @saved_keys[str]
    else
      @search = str
      throw 'resolve'
  
  # This is a bit of syntactic sugar. It's the equivalent of:
  # 
  #     x = @get key
  #     @for_reals()
  #     x
  get_now: ->
    value = @get.apply this, arguments
    @for_reals()
    value

  # If a klass is given, construct a new one; otherwise, just return
  # the raw value.
  build: (value, klass) ->
    klass ?= @wrapper_class
    if klass? then @bless(new klass(value)) else value
  
  # Extend the given object with the context methods of a worker,
  # in addition to a recursive blessing.
  bless: (object) ->
    for method in ['get', 'emit', 'for_reals', 'get_now', 'keys']
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
    console.log 'emit', @key, key, json # XXX
    @db.set key, JSON.stringify(json)
    @db.publish @resp_channel, key

  # If we've seen this `@for_reals` before, then blow right past it.
  # Otherwise, abort the runner function and start over (after checking
  # that our dependencies are met).
  for_reals: ->
    if @stage == @last_stage
      if @deps.length
        if @is_async
          @resolve()
          return false          
        else
          throw 'resolve'
      @last_stage++
    @stage++
    true

  # Attempt to run the runner function. If a call to `@for_reals` causes
  # us to abort, then attempt to resolve the dependencies.
  run: ->
    try
      @clear()
      @process()
    catch err
      @caught err

  # Reset information about this run, including:
  # 
  # * `@stage`: how many calls to `@for_reals` we've seen
  # * `@deps`: a list of new dependencies
  # * `@emitted`: whether `@emit` has been called.
  # * `@search`: the key search currently requested
  # * `@is_async`: whether the worker is in async mode
  clear: ->
    @stage = 0
    @key_index = 0
    @deps = []
    @emitted = false
    @search = null
    @is_async = false
    Worker.clear_callback?.apply this

  # If the caught error is from a `@for_reals`, then try to resolve
  # dependencies.
  caught: (err) ->
    if err == 'resolve'
      @resolve()
    else if err.is_cycle
      @cycle_failure err.key
    else
      @error err

  # Let the dispatcher know that we failed to handle a cycle. Eventually the dispatcher
  # may resolve that conflict and re-run us anyway.
  cycle_failure: (key) ->
    @failed_cycle = true
    @failed_raise[key] = true
    
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
    result = @runner.apply this, @args
    return @resolve() if @deps.length
    @finish result unless @is_async
  
  # We're done!
  finish: (result) ->
    Worker.finish_callback?.apply this
    num_workers--
    @queue.finish @key
    @emit @key, (result ? null) unless @emitted

  # Compare the provided values against our current dependencies.
  # Missing dependencies are returned in an array.
  check_values: (arr) ->
    bad = []
    for dep, i in @deps
      @cache[dep] = JSON.parse arr[i]
      bad.push dep unless @cache[dep]?
    bad

  # The first step in resolving dependencies from a `@for_reals` is
  # to record that there's one more of them to get through, then to check
  # the dependencies.
  resolve: ->
    if @search
      @db.keys @search, (e, keys) =>
        @saved_keys[@search] = keys
        @run()
    else
      @last_stage++
      @get_deps()

  # Ask redis to provide values for our dependencies. If any are missing,
  # send a request to the dispatcher; otherwise, resume trying to run the
  # main function.
  get_deps: (force = false) ->
    throw "No dependencies to get: #{@key}" unless @deps.length
    @db.mget @deps, (err, arr) =>
      return @error err if err
      bad = @check_values arr
      if bad.length && !force
        @request_missing bad
      else
        @run()

  # Ask the dispatcher to providethe given keys by publishing on the
  # `requests` channel. Then block-wait to be signalled by a response
  # on a resume key. Once we get that response, try again to fetch the
  # dependencies (which should all be present).
  request_missing: (keys) ->
    request = [@key, keys...].join consts.key_sep
    @db.publish @req_channel, request

  # The dispatcher said to resume, so go look for the missing values again. If
  # we're resuming from a cycle failure, go grab the key.
  resume: ->
    if @failed_cycle
      @unblock()
    else
      @get_deps true
  
  # Get the blocker key and continue
  unblock: ->
    @failed_cycle = false
    keys = _.keys @cycle
    @db.mget keys, (err, arr) =>
      return @error err if err
      for key, index in keys
        if !@tested_raise[key] || @failed_raise[key]
          @cache[key] = JSON.parse(arr[index])
          delete @cycle[key]
      @run()
  
  # The given key is part of a cycle and we depend on it. Mark it as being cyclical,
  # so that the next request to `@get` will raise a CycleError. This error can be caught,
  # in which case this key will remain in `@cycle`. If uncaught, and if some other
  # worker breaks the cycle, the dispatcher may allow us to resolve this key. In that
  # case, it will be removed from `@cycle` by `@unblock()`
  cycle_detected: (key) ->
    @cycle[key] = true
    @tested_raise[key] = false
  
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