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
    [@prefix, @args...] = @key.split consts.arg_sep
    @_pubsub = @queue._worker_pubsub
    @_kv = @queue._worker_kv
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
    if @_all
      @gets.push args
      return
    @on_cycle = _.callback args
    opts = _.opts args
    key = args.join consts.arg_sep
    @_last_key = key
    @notify_dep key
    if saved = @sticky[key] ? @cache[key]
      return saved
    @_kv.get key, (err, val) =>
      if val
        @fiber.run [val]
      else
        @request_keys [key]
    vals = @yield()
    if @cycling
      @cycling = false
      val = @on_cycle.apply @target()
    else
      val = @build vals[0], opts.as
    @cache[key] = val
    @sticky[key] = val if opts.sticky
    val
  
  # Get multiple keys in parallel, and return them in an array
  all: (fun) ->
    # return fun.apply @target()
    @_all = true
    @gets = []
    fun.apply @target()
    @_all = false
    @_get_all()
  
  # Request multiple keys in parallel, but don't bother to actually
  # collect the results.
  each: (fun) ->
    # return fun.apply @target()
    @_all = true
    @gets = []
    fun.apply @target()
    @_all = false
    @_ensure_all()
  
  # Request all given but missing keys
  _ensure_all: ->
    opts = _.map @gets, (args) -> _.opts args
    keys = _.map @gets, (args) -> args.join consts.arg_sep
    rem = keys.length
    needed = []
    missing = []
    for key in keys
      unless @sticky[key]? || @cache[key]?
        needed.push key
    return unless rem = needed.length
    finish = =>
      @_skip_get_on_resume = true
      if missing.length
        @request_keys missing
      else
        @fiber.run()
    for key in needed
      do (key) =>
        @_kv.exists key, (err, exists) ->
          missing.push key unless exists
          finish() unless --rem
    @yield()
  
  # Get all requested keys
  _get_all: ->
    opts = _.map @gets, (args) -> _.opts args
    keys = _.map @gets, (args) -> args.join consts.arg_sep
    values = {}
    needed = []
    missing = []
    for key in keys
      if val = @sticky[key] ? @cache[key]
        values[key] = val
      else
        needed.push key
    if needed.length
      @_kv.get_all needed, (err, vals) =>
        for val, i in vals
          if val
            values[needed[i]] = val
          else
            missing.push needed[i]
        if missing.length
          @request_keys missing
        else
          @fiber.run []
      for val, i in @yield()
        values[missing[i]] = val
    for key, i in keys
      val = @build values[key], opts[i].as
      @cache[key] ?= val
      @sticky[key] ?= val if opts[i].sticky
      val

  # Notify the dispatcher of our dependency (regardless of whether we're
  # going to request that key).
  notify_dep: (key) ->
    msg = ['!dep', @key, key].join consts.key_sep
    @_pubsub.publish @req_channel, msg
  
  # Search for the given keys in the database, then remember them.
  keys: (str) ->
    @_kv.keys str, (err, arr) =>
      @fiber.run arr
    @yield()
  
  yield: ->
    _.tap yield(), => Worker.current = this
  
  # If a klass is given, construct a new one; otherwise, just return
  # the raw value.
  build: (value, klass) ->
    klass ?= @wrapper_class
    if klass? then @bless(new klass(value)) else value
  
  # Extend the given object with the context methods of a worker,
  # in addition to a recursive blessing.
  bless: (object) ->
    proto = object
    while proto.__proto__ != Worker.Workspace.prototype
      if proto.__proto__ == {}.__proto__
        proto.__proto__ = Worker.Workspace.prototype
        break
      proto = proto.__proto__
    object

  # Produce `value` as a result for `key`. This both puts the result
  # in redis under the key and tells the dispatcher (via the `responses`
  # channel) that the job is done.
  emit: (args..., value) ->
    @emitted = true
    key = args.join consts.arg_sep
    json = value?.toJSON?() ? value
    @_kv.set key, json, =>
      @_pubsub.publish @resp_channel, key
      @fiber.run() if @fiber
    @yield() if @fiber

  # Attempt to run the runner function.
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
    @_kv.set 'fatal', message

  # Call the runner. We optionally
  # emit the result of the function (if nothing has been emitted yet).
  process: ->
    Worker.current = this
    result = @runner.apply(@target(), @args)
    @finish result
  
  target: ->
    return @workspace if @workspace
    @workspace = new Worker.Workspace
    if params = @queue.params_for(@prefix)
      for param, i in params
        @workspace[param] = @args[i]
    @workspace
  
  atomic: (key, value) ->
    @_kv.atomic_set key, value, (err, real) =>
      @fiber.run real
    @yield()
  
  # We're done!
  finish: (result) ->
    Worker.finish_callback?.apply this
    num_workers--
    @queue.finish @key
    @emit @key, (result ? null) unless @emitted
    # console.log 'finish', @key # XXX
    @fiber = null

  # Ask the dispatcher to providethe given keys by publishing on the
  # `requests` channel. Then block-wait to be signalled by a response
  # on a resume key. Once we get that response, try again to fetch the
  # dependencies (which should all be present).
  request_keys: (keys) ->
    @requested = keys
    msg = [@key, keys...].join consts.key_sep
    @_pubsub.publish @req_channel, msg

  # The dispatcher said to resume, so go look for the missing values again. If
  # we're resuming from a cycle failure, go grab the key.
  resume: ->
    @fiber ?= Fiber => @run()
    if @_skip_get_on_resume
      @_skip_get_on_resume = false
      return @fiber.run()
    @_kv.get_all @requested, (err, vals) =>
      console.log 'fail', @key unless @fiber
      @fiber.run vals
  
  cycle: ->
    if @on_cycle
      @cycling = true
      @fiber.run()
    else
      @fiber = null
    # else
    #   @queue.finish @key
    #   @fiber = null
    # XXX: if no @on_cycle is defined, we basically just never
    # run @fiber again. but it would be nice to actually dispose
    # of it...
  
  # Set the default wrapper class, which is overridden by `as: `
  wrapper: (klass) ->
    @wrapper_class = klass
  
  # Return the current worker
  worker: ->
    Worker.current


module.exports = Worker
