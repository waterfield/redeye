require 'fibers'
msgpack = require 'msgpack'
_ = require './util'
pool = require './pool'
{ DependencyError, MultiError } = require './errors'

# One worker is constructor for every task coming
# through the work queue(s). It maintains a local cache
# of dependency values, a workspace for running the
# key code, and contains the core API for redeye.
# It runs worker code in a fiber, yielding the fiber
# every time asynchronous work (such as waiting for
# dependencies) must be done.
class Worker

  # Break key into prefix and arguments, and set up worker cache.
  constructor: (@key, @queue, @old_deps, @manager) ->
    [@prefix, @args...] = @key.split ':'
    @workspace = new Worker.Workspace
    params = @manager.params[@prefix] || []
    @workspace[param] = @args[i] for param, i in params
    Worker.clear_callback?.apply this
    @cache = {}
    @deps = []

  # API METHODS
  # ===========

  # `@async (callback) -> ... callback(err, value)`
  #
  # Uses paired `@yield` and `@resume` to perform an asynchronous
  # function outside the worker fiber. The function should call the
  # callback with arguments `error` and `value`, where `value`
  # will be the result of the `@async` call.
  async: (body) ->
    @release_db (err) =>
      @resume err if err
      body (err1, value) =>
        @acquire_db (err2) =>
          @resume (err1 || err2), value
      @yield()

  # `@log(label, payload)`
  #
  # Use the `Manager#log` function to log a message for this key.
  # The resulting message will include `@key` in the payload.
  log: (label, payload) ->
    @mananger.log @key, label, payload

  # `@get(prefix, args..., opts, callback)`
  #
  # Retrieve another key given the named prefix and its arguments.
  # Three things can happen with this request under normal operation:
  #
  # * The key is already in our local `@cache`; its already-built
  #   value is just returned.
  # * The key is available in the database. It is retrieved and built
  #   into an object if applicable. `Manager#require` is called to
  #   indicate this worker's dependency on the key. The built value is
  #   returned.
  # * The key is not available in the database. `Manager#require` is
  #   called to indicate the dependency. Then, `Manager#enqueue` is
  #   called with the key to create a new job. Once the work queue is
  #   notified of completion of the key with the `ready` control message,
  #   `@get` will resume.
  #
  # The following options can be provided:
  #
  # * `as`: if provided, should be a class. The raw value of the key
  #   will be passed to its constructor. A common pattern for this is:
  #
  #    class Wrapper extends Workspace
  #
  #      constructor: (raw) ->
  #        super()
  #        _.extend @, raw
  #
  #   By extending `Workspace`, the wrapper class has access to the
  #   worker's API methods.
  #
  # `@get` can return two kinds of errors:
  #
  # * `DependencyError`: the requested key had an error which is being
  #   bubbled up to this worker
  # * `CycleError`: making the given request would result in a cycle
  #
  # Both kinds of errors can be caught and handled normally.
  get: (args...) ->
    return @defer_get(args) if @in_each
    {prefix, opts, key} = @parse_args args
    return saved if saved = @cache[key]
    @deps.push key
    @require [key], (err, values) =>
      if err
        @resume err
      else if values[0]
        @resume null, values
      else
        @wait [key]
    @cache[key] = @build @yield()[0], prefix, opts

  # Search for the given keys in the database and return them.
  keys: (pattern) ->
    @async (callback) =>
      @db.keys pattern, callback

  # Atomically set the given key to a value.
  atomic: (key, value) ->
    @async (callback) =>
      @db.setnx key, value, callback

  # `@yield()`
  #
  # Suspend the worker fiber until `@resume` is called. If `@resume`
  # is called with an error, throw that error on resume; otherwise,
  # `@yield` will return the value passed to `@resume`.
  yield: ->
    [ err, value ] = yield()
    throw err if err
    value

  # `@resume(error, value)`
  #
  # Resume a suspended worker fiber. If `error` is provided, it will
  # be thrown from the fiber. Otherwise, `value` will be returned from
  # the last call to `@yield`. If the fiber throws an error, record
  # that error as the result of the worker for this key.
  resume: (err, value) ->
    if @dirty
      @fiber = null
      return
    if @waiting_for
      @stop_waiting()
      return
    Worker.current = this
    try
      @fiber.run [err, value]
    catch e
      @error e

  # `@run()`
  #
  # Start running the worker for the first time. Sets up the
  # worker fiber, clears the worker, and starts running the
  # worker body. Uses `@resume` so that errors are properly
  # caught. Take the result of the worker body as the value
  # of this key and passing it to `@finish`. If there is no
  # worker body defined for this (non-input) prefix, throw an
  # error.
  run: ->
    @fiber = Fiber =>
      @acquire_db (err) =>
        if err
          throw new Error err
        else if runner = @manager.runners[@prefix]
          @finish runner.apply(@workspace, @args)
        else if @manager.is_input[@prefix]
          @finish null
        else
          throw new Error "No runner for prefix '#{@prefix}'"
    @resume()

  # `@with key1: [vals...], key2: [vals...], -> ...`
  #
  # Iterate the keys over the cross-product of the provided values.
  # For each iteration, call the given function. This function is used
  # by `@all` and `@each`, but can also be used directly. The function
  # will be called in the context of the workspace.
  #
  # In each iteration, the keys are recorded into the workspace object.
  # That means if you can do something like the following:
  #
  #     @with foo: ['bar', 'baz'], ->
  #       console.log @foo
  #
  # which would print 'bar', then 'baz'.
  #
  # Because `Workspace` provides a simplified API for accessing other
  # worker keys and inputs, `@with` can be used to provide argument
  # context like so:
  #
  #     # @worker 'foo', 'id', -> ...
  #     @with id: [1,2], -> @foo()
  #
  # which would first `@get('foo', 1)` and then `@get('foo', 2)`.
  #
  # NOTE: Please do not nest calls to `@with`, `@each`, or `@all`!
  with: (hash, fun, keys) ->
    @in_each = true
    unless keys
      unless fun
        fun = hash
        hash = {}
      keys = _.keys(hash)
      @context = {}
      @context_list = []
      @gets = []
    if key = keys.shift()
      for val in hash[key]
        @context[key] = val
        @workspace[key] = val
        @with hash, fun, keys
        delete @context[key]
      keys.unshift key
    else
      fun.apply @workspace
    @in_each = false

  # `@each key1: [vals...], key2: [vals...], -> ...`
  #
  # Uses the same syntax as `@with`. Each `@get` call from within
  # the body is recorded. Before `@each` returns, it ensures all
  # these dependencies are met, but does not bother to actually
  # return them. Instead it will return the total number of `@get`
  # dependencies. If one or more of the dependencies has an error,
  # then this call will throw a `MultiError` with the error keys.
  # Each error within the `MultiError` records the `context` under
  # which it was requested with `@get`.
  #
  # All dependencies will be satisfied in parallel.
  #
  # TODO: This could be more efficient. Hard to check for errors though.
  each: (hash, fun) ->
    @all(hash, fun).length

  # `@all key1: [vals...], key2: [vals...], -> ...`
  #
  # Uses the same syntax as `@with`. Each `@get` call from within
  # the body is recorded. Before `@all` returns, it ensures all
  # these dependencies are met, and then returns them all in an
  # array. If one or more of the dependencies has an error,
  # then this call will throw a `MultiError` with the error keys.
  # Each error within the `MultiError` records the `context` under
  # which it was requested with `@get`.
  #
  # All dependencies will be satisfied in parallel.
  all: (hash, fun) ->
    @with hash, fun
    @record_all_opts()
    @find_needed_keys()
    @find_missing_keys()
    @get_missing_keys()
    @build_all()

  # INTERNAL METHODS
  # ================

  # Get a database client from the pool, set `@db`, and call back.
  acquire_db: (callback) ->
    if @db
      err = new Error "Tried to acquire db when we already had one"
      return callback err
    pool.acquire (err, @db) =>
      callback err

  # Release our database client back to the pool.
  release_db: (callback) ->
    unless @db
      err = new Error "Tried to release db when we didn't have one"
      return callback err
    pool.release @db
    @db = null
    callback()

  # The worker is done and this is its value. Convert using `toJSON` if present,
  # set the key's value, then tell the queue that the key should be released.
  finish: (value) ->
    @fiber = null
    return if @dirty
    value = value?.toJSON?() ? value
    value = msgpack.pack value
    @db.set @key, value, =>
      @fix_source_targets =>
        @release_db =>
          @manager.finish @key
          Worker.finish_callback?.apply this

  # We may have dropped some dependencies; in that case, remove us as a
  # target from the dropped ones.
  fix_source_targets: (callback) ->
    bad_deps = []
    for dep in @old_deps
      unless dep in @deps
        bad_deps.push dep
    return callback() unless bad_deps.length
    m = @db.multi()
    for dep in bad_deps
      m.srem 'targets:'+dep, @key
    m.exec callback

  # Convert the given error message or object into a value
  # suitable for exception bubbling, then set that error as the
  # result of this key with `@finish`.
  error: (err) ->
    if err.cycle
      @release_db (err) =>
        throw err if err
        @manager.cycle @key, err
        @fiber = null
    else
      trace = err.stack ? err
      error = err.get_tail?() ? [{ trace, @key, @slice }]
      @finish { error }

  # Parse the @get arguments into the prefix, key arguments,
  # and options.
  parse_args: (args) ->
    prefix = args[0]
    @on_cycle = _.callback args
    opts = _.opts args
    key = args.join ':'
    { prefix, opts, key }

  # Inform the work queue of this dependency.
  require: (sources, callback) ->
    @manager.require @queue, sources, @key, callback

  # Tell the work queue to resume us when all the given dependencies are ready.
  # Once resumed, look up the value of all the dependencies, and resume with them.
  wait: (deps) ->
    @waiting_for = (new Buffer dep for dep in deps)
    @release_db (err) =>
      return @resume err if err
      @manager.wait deps, @key

  # We have resumed after the last `@wait`, so look up the keys we're waiting on
  # and resume the fiber with them.
  stop_waiting: ->
    @acquire_db (err) =>
      return @resume err if err
      @db.mget @waiting_for, (err, arr) =>
        @waiting_for = null
        arr = (msgpack.unpack buf for buf in arr) unless err
        @resume err, arr

  # Because we're in an `@each` or `@all` block, don't attempt
  # to get the key yet; instead, just record the context and
  # arguments at the time `@get` was called, to be retrieved in
  # a batch later.
  defer_get: (args) ->
    @context_list.push _.clone(@context)
    @gets.push args

  # Check if the returned value is an error. If so, prepend the
  # current context to the error trace and re-throw it.
  #
  # Otherwise, if there is an `as` option specified for the value's
  # prefix, construct an object with the wrapper class.
  build: (value, prefix, opts) ->
    return value unless value?
    @test_for_error value
    if wrapper = opts.as || @manager.as[prefix]
      new klass(value)
    else
      value

  # Test if the given value looks like a redeye recorded error;
  # that is, it is an object with an array 'error' field. If it
  # is, throw a `DependencyError`.
  test_for_error: (value) ->
    if _.isArray value.error
      throw new DependencyError @, value.error

  # Convert the arguments for each parallel `@get` request into a hash containing
  # `opts`, `prefix`, and `index`, where `index` is the index of the corresponding
  # `get`. The hash is keyed by redeye key.
  record_all_opts: ->
    @key_prefixes = {}
    @key_opts = {}
    @all_keys = []
    @pending = []
    for args, index in @gets
      opts = _.opts args
      key = args.join ':'
      prefix = args[0]
      @key_opts[key] = { prefix, opts, index }
      @all_keys.push key

  # From the keys requested by `@all`/`@each`, determine which ones are not already
  # cached locally by the worker, and store them in `@needed`; this array holds alternately
  # the lock name and the key itself, suitable for passing to `redis.mget`.
  # Also use `Manager.require` to record each dependency.
  find_needed_keys: ->
    @needed = []
    for key in @all_keys
      continue if @cache[key]?
      @needed.push key
      @deps.push key

  # From the keys in `@needed`, ask the database which are available by finding both the
  # lock state and the value of the key. For any unavailable key, put that key in the `@missing`
  # list. For keys which are already available, append the key and value to the `@pending` list.
  find_missing_keys: ->
    @missing = []
    return unless @needed.length
    @require @needed, (err, values) =>
      return @resume err if err
      for key, index in @needed
        if value = values[index]
          @pending.push [key, value]
        else
          @missing.push key
      @resume()
    @yield()

  # For each missing key, if the key is unlocked, ask the queue to enqueue that
  # key as a job. Finally, for all missing keys, yield and ask the queue to resume the
  # worker when they are available. Take the provided values from resume and put
  # them on the `@pending` list.
  get_missing_keys: ->
    return unless @missing.length
    @wait @missing
    for value, index in @yield()
      @pending.push [@missing[index], value]

  # Keys from redis or from resuming from the queue are present in the `@pending`
  # list. For each, test the value for errors and build the key using its prefix
  # and options. If one or more values are errors, add those errors to a new
  # `MultiError` object and throw it. Make sure the context from each error key
  # is recorded on its error object. If there are no errors, return the list of values.
  build_all: ->
    multi = null
    for item in @pending
      [key, value] = item
      { opts, prefix, index } = @key_opts[key]
      try
        @cache[key] = @build value, prefix, opts
      catch err
        err.context = @context_list[index]
        multi ||= new MultiError @
        multi.add err
    throw multi if multi
    @cache[key] for key in @all_keys

module.exports = Worker
