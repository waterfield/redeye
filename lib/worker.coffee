{DependencyError, MultiError} = require './errors'
consts = require './consts'
db = require './db'
_ = require './util'
require 'fibers'

# The worker class is the context under which runner functions are run.
class Worker

  # Find the runner for the `@key`. The key is in the format:
  #
  #     prefix:arg1:arg2:...
  constructor: (@key, @queue, @sticky) ->
    [@prefix, @args...] = @key.split consts.arg_sep
    @_cycle_timeout = 10000
    @_pubsub = @queue._worker_pubsub
    @_kv = @queue._worker_kv
    @_counters = {}
    @slice = @queue.options.db_index
    @_pubsub = db.pub_sub @queue.options
    @cache = {}
    unless @runner = @queue.runners[@prefix]
      @emit @key, null
      unless @queue._is_input[@prefix]
        console.log "no runner for '#{@prefix}' (#{@key})"
      throw 'no_runner'

  next_unique_id: (key=@key) ->
    @_counters[key] ?= 1
    "#{key}-#{@_counters[key]++}"

  log: (label, payload) ->
    @queue.log @key, label, payload

  get: (args...) ->
    if @_all
      @_context_list.push _.clone(@_context)
      return @gets.push(args)
    { prefix, opts, key } = @_parse_args args
    return saved if saved = @cache[key]
    @notify_dep key
    vals = @_get key
    val = if @cycling
      @cycling = false
      @on_cycle.apply @target()
    else
      @build vals[0], @_as(opts, prefix)
    @cache[key] = val
    val

  _get: (key) ->
    @_kv.get '_lock:'+key, (err, lock) =>
      return @queue.error(err) if err
      if lock == 'ready'
        @_kv.get key, (err, val) =>
          @_run [val]
      else
        @_wait_on [key]
        @queue.enqueue_job @key, key unless lock
    @yield()

  _wait_on: (deps) ->
    @_waiting_on = deps
    @queue.listen_for deps, @key

  _queue_job: (key) ->
    @_kv.setnx '_lock:'+key, 'queued', (err, set) ->
      return @queue.error(err) if err
      @_queue.lpush 'jobs', key if set

  # Parse the @get arguments into the prefix, key arguments,
  # and options.
  _parse_args: (args) ->
    prefix = args[0]
    @on_cycle = _.callback args
    opts = _.opts args
    key = args.join consts.arg_sep
    { prefix, opts, key }

  # # Get multiple keys in parallel, and return them in an array
  # all: (hash, fun) ->
  #   @_all = true
  #   @gets = []
  #   @_context_list = []
  #   @_context = {}
  #   @_apply_many hash, fun
  #   @_all = false
  #   @_get_all()

  # # Request multiple keys in parallel, but don't bother to actually
  # # collect the results.
  # each: (hash, fun) ->
  #   @_all = true
  #   @_context_list = []
  #   @_context = {}
  #   @gets = []
  #   @_apply_many hash, fun
  #   @_all = false
  #   @_ensure_all()
  #   @gets.length

  # _apply_many: (hash, fun) ->
  #   if fun
  #     @with hash, fun
  #   else
  #     hash.apply @workspace

  with: (hash, fun) ->
    @_with hash, fun, _.keys(hash)

  _with: (hash, fun, keys) ->
    if key = keys.shift()
      for val in hash[key]
        @_context[key] = val
        @workspace[key] = val
        @_with hash, fun, keys
        delete @_context[key]
      keys.unshift key
    else
      fun.apply @workspace

  # # Request all given but missing keys
  # _ensure_all: ->
  #   opts = _.map @gets, (args) -> _.opts args
  #   keys = _.map @gets, (args) -> args.join consts.arg_sep
  #   rem = keys.length
  #   needed = []
  #   missing = []
  #   for key in keys
  #     unless @sticky[key]? || @cache[key]?
  #       needed.push key
  #   return unless rem = needed.length
  #   finish = =>
  #     # NOTE: had to comment this out so that on @resume(),
  #     # we load the values and test them for errors. But there's
  #     # probably a better way of doing that.
  #     #
  #     # @_skip_get_on_resume = true
  #     if missing.length
  #       @request_keys missing
  #     else
  #       @_run []
  #   for key in needed
  #     do (key) =>
  #       @_kv.exists key, (err, exists) ->
  #         missing.push key unless exists
  #         finish() unless --rem
  #   multi = null
  #   for val, i in @yield()
  #     try
  #       @_test_for_error(val) if val
  #     catch err
  #       err.context = @_context_list[i]
  #       (multi ||= new MultiError @).add err
  #   throw multi if multi

  # # Get all requested keys
  # _get_all: ->
  #   opts = _.map @gets, (args) -> _.opts args
  #   keys = _.map @gets, (args) -> args.join consts.arg_sep
  #   prefixes = _.map @gets, (args) -> args[0]
  #   values = {}
  #   needed = []
  #   missing = []
  #   for key in keys
  #     if val = @sticky[key] ? @cache[key]
  #       values[key] = val
  #     else
  #       needed.push key
  #   if needed.length
  #     @_kv.get_all needed, (err, vals) =>
  #       for val, i in vals
  #         if val
  #           values[needed[i]] = val
  #         else
  #           missing.push needed[i]
  #       if missing.length
  #         @request_keys missing
  #       else
  #         @_run []
  #     for val, i in @yield()
  #       values[missing[i]] = val
  #   multi = null
  #   built = for key, i in keys
  #     try
  #       val = @build values[key], @_as(opts[i], prefixes[i])
  #       @cache[key] ?= val
  #       @sticky[key] ?= val if opts[i].sticky
  #       val
  #     catch err
  #       err.context = @_context_list[i]
  #       (multi ||= new MultiError @).add err
  #   throw multi if multi
  #   built

  _as: (opts, prefix) ->
    opts.as || @queue._as[prefix]

  # Notify the dispatcher of our dependency (regardless of whether we're
  # going to request that key).
  notify_dep: (key) ->
    @queue.log @key, 'redeye:require', source: key, target: @key
    @_kv.sadd "sources:#{@key}", key
    @_kv.sadd "targets:#{key}", @key
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
      throw new DependencyError @, value.error

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
      state = if value?.error then 'error' else 'ready'
      @queue.finish_key
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
    error = err.get_tail?() ? [{ trace, @key, @slice }]
    @_emit @key, { error }

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
    @queue.finish_worker @key
    @fiber = null
    @emit @key, (result ? null) unless @emitted

  resume: ->
    @fiber ?= Fiber => @run()
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
