{DependencyError, MultiError} = require './errors'
consts = require './consts'
db = require './db'
_ = require './util'
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
    @_counters = {}
    @slice = @queue.options.db_index
    @req_channel = _('requests').namespace @slice
    @resp_channel = _('responses').namespace @slice
    @cache = {}
    unless @runner = @queue.runners[@prefix]
      @emit @key, null
      console.log "no runner for '#{@prefix}' (#{@key})"
      throw 'no_runner'
    num_workers++

  next_unique_id: (key=@key) ->
    @_counters[key] ?= 1
    "#{key}-#{@_counters[key]++}"

  # If we've already seen this `@get` before, then return the actual
  # value we've received (which we know we got because otherwise we
  # wouldn't be running again). Otherwise, just mark this dependency
  # and return `undefined`.
  get: (args...) ->
    return @gets.push(args) if @_all
    { prefix, opts, key } = @_parse_args args
    @_last_key = key
    @_check_caches key
    vals = @_get key
    val = if @cycling
      @cycling = false
      @on_cycle.apply @target()
    else
      @build vals[0], @_as(opts, prefix)
    @cache[key] = val
    @sticky[key] = val if opts.sticky
    val

  # Request a key from the database; if it's not fond, request
  # it from the dispatcher. Resume the fiber only when the value is
  # found.
  _get: (key) ->
    @_kv.get key, (err, val) =>
      if val
        @_run [val]
      else
        @request_keys [key]
    @yield()

  # Parse the @get arguments into the prefix, key arguments,
  # and options.
  _parse_args: (args) ->
    prefix = args[0]
    @on_cycle = _.callback args
    opts = _.opts args
    key = args.join consts.arg_sep
    { prefix, opts, key }

  # If we've already requested the key, just return its last known
  # value. Otherwise, mark the key as a dependency. Even so, return
  # the value from the sticky cache, if present.
  _check_caches: (key) ->
    return saved if saved = @cache[key]
    @notify_dep key
    return saved if saved = @sticky[key]

  # Get multiple keys in parallel, and return them in an array
  all: (hash, fun) ->
    @_all = true
    @gets = []
    @_apply_many hash, fun
    @_all = false
    @_get_all()

  # Request multiple keys in parallel, but don't bother to actually
  # collect the results.
  each: (hash, fun) ->
    @_all = true
    @gets = []
    @_apply_many hash, fun
    @_all = false
    @_ensure_all()
    @gets.length

  _apply_many: (hash, fun) ->
    if fun
      for key, array of hash
        for entry in array
          @workspace[key] = entry
          fun.apply @workspace
    else
      hash.apply @workspace


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
      # NOTE: had to comment this out so that on @resume(),
      # we load the values and test them for errors. But there's
      # probably a better way of doing that.
      #
      # @_skip_get_on_resume = true

      if missing.length
        @request_keys missing
      else
        @_run []
    for key in needed
      do (key) =>
        @_kv.exists key, (err, exists) ->
          missing.push key unless exists
          finish() unless --rem
    multi = null
    for val, i in @yield()
      try
        @_test_for_error(val) if val
      catch err
        (multi ||= new MultiError).add err, i
    throw multi if multi

  # Get all requested keys
  _get_all: ->
    opts = _.map @gets, (args) -> _.opts args
    keys = _.map @gets, (args) -> args.join consts.arg_sep
    prefixes = _.map @gets, (args) -> args[0]
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
          @_run []
      for val, i in @yield()
        values[missing[i]] = val
    multi = null
    built = for key, i in keys
      try
        val = @build values[key], @_as(opts[i], prefixes[i])
        @cache[key] ?= val
        @sticky[key] ?= val if opts[i].sticky
        val
      catch err
        (multi ||= new MultiError).add err, i
    throw multi if multi
    built

  _as: (opts, prefix) ->
    opts.as || @queue._as[prefix]

  # Notify the dispatcher of our dependency (regardless of whether we're
  # going to request that key).
  notify_dep: (key) ->
    msg = ['!dep', @key, key].join consts.key_sep
    @_pubsub.publish @req_channel, msg
    @got(key) if @got # can be mixed in

  # Search for the given keys in the database, then remember them.
  keys: (str) ->
    @_kv.keys str, (err, arr) => @_run arr
    @yield()

  yield: ->
    _.tap yield(), => Worker.current = this

  # If a klass is given, construct a new one; otherwise, just return
  # the raw value.
  build: (value, klass) ->
    return value unless value?
    @_test_for_error value
    klass ?= @wrapper_class
    if klass? then @bless(new klass(value)) else value

  _test_for_error: (value) ->
    if _.isArray value.error
      throw new DependencyError value.error

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
    @_emit key, value, => @_run()
    @yield() if @fiber

  _emit: (key, value, callback) ->
    json = value?.toJSON?() ? value
    @_kv.set key, json, =>
      @_pubsub.publish @resp_channel, key
      callback() if callback

  # Attempt to run the runner function.
  run: ->
    @fiber = Fiber =>
      # console.log "ENTER (#{@timestamp()}): #{@key}"
      @clear()
      @process()
      # console.log "LEAVE (#{@timestamp()}): #{@key}"
    @_run()

  # Perform the code block asynchronously, outside the
  # main fiber. The code block should take a callback
  # and, when the asynchronous operation is complete,
  # call it with (error, result). The result is returned
  # from @async, and the error, if any, is thrown.
  async: (body) ->
    body (args...) => @_run(args)
    [err, result] = @yield()
    throw err if err
    result

  # Resume the fiber, catching any errors
  _run: (arg) ->
    try
      @fiber.run arg if @fiber
    catch e
      @error e

  timestamp: ->
    new Date().toJSON()

  # Reset information about this run, including:
  #
  # * `@emitted`: whether `@emit` has been called.
  clear: ->
    @emitted = false
    Worker.clear_callback?.apply this

  # Mark that a fatal exception occurred
  error: (err) ->
    trace = err.stack ? err
    tail = err.get_tail?() ? []
    tail.unshift { trace, @key, @slice }
    @_emit @key, error: tail

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
    @_kv.atomic_set key, value, (err, real) => @_run real
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
      return @_run()
    @_kv.get_all @requested, (err, vals) =>
      console.log 'fail', @key unless @fiber
      @_run(vals)

  cycle: ->
    if @on_cycle
      @cycling = true
      @_run()
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
