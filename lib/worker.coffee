{DependencyError, MultiError} = require './errors'
consts = require './consts'
msgpack = require 'msgpack'
_ = require './util'
require 'fibers'

class Worker

  # Break key into prefix and arguments, and set up worker cache.
  constructor: (@key, @queue) ->
    [@prefix, @args...] = @key.split consts.arg_sep
    @kv = @queue._worker_kv.redis
    @slice = @queue.options.db_index

  # API METHODS
  # ===========

  # `@async (callback) -> ... callback(err, value)`
  #
  # Uses paired `@yield` and `@resume` to perform an asynchronous
  # function outside the worker fiber. The function should call the
  # callback with arguments `error` and `value`, where `value`
  # will be the result of the `@async` call.
  async: (body) ->
    body (err, value) =>
      @resume err, value
    @yield()

  # `@log(label, payload)`
  #
  # Use the `WorkQueue#log` function to log a message for this key.
  # The resulting message will include `@key` in the payload.
  log: (label, payload) ->
    @queue.log @key, label, payload

  # `@get(prefix, args..., opts, callback)`
  #
  # Retrieve another key given the named prefix and its arguments.
  # Three things can happen with this request under normal operation:
  #
  # * The key is already in our local `@cache`; its already-built
  #   value is just returned.
  # * The key is available in the database. It is retrieved and built
  #   into an object if applicable. `WorkQueue#require` is called to
  #   indicate this worker's dependency on the key. The built value is
  #   returned.
  # * The key is not available in the database. `WorkQueue#require` is
  #   called to indicate the dependency. Then, `WorkQueue#enqueue` is
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
  # If a callback is provided, it will be called in the event the worker
  # detects a cycle on the requested key. The callback has access to all
  # normal API methods.
  get: (args...) ->
    return @defer_get(args) if @in_each
    {prefix, opts, key} = @parse_args args
    return saved if saved = @cache[key]
    @require key
    @kv.mget 'lock:'+key, key, (err, arr) =>
      return @resume err if err
      [lock, value] = arr
      lock = lock.toString() if lock
      value = msgpack.unpack value if value
      if lock == 'ready'
        @resume null, [value]
      else
        @wait [key]
        @queue.enqueue key, @key, lock
    values = @yield()
    @on_cycle = null
    value = if @cycling
      @cycling = false
      @on_cycle.apply @workspace
    else
      @build values[0], prefix, opts
    @cache[key] = value
    value

  # Search for the given keys in the database and return them.
  keys: (pattern) ->
    @async (callback) =>
      @kv.keys pattern, (err, keys) =>
        return callback err if err
        keys = (key.toString() for key in keys)
        callback null, keys

  # Atomically set the given key to a value.
  atomic: (key, value) ->
    @async (callback) =>
      @kv.setnx key, value, callback

  # A cycle was detected on this worker while requesting another key.
  # If the current request supports cycle recovery, then continue
  # processing; otherwise, do nothing yet, because some other worker
  # in the cycle may complete and break the cycle.
  cycle: ->
    if @on_cycle
      @cycling = true
      @resume()

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
    return @stop_waiting() if @waiting_for
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
  # caught.
  run: ->
    @fiber = Fiber =>
      @clear()
      @process()
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
      keys = _.keys(hash)
      @context = {}
      @context_list = []
      @gets = []
    if key = keys.shift()
      for val in hash[key]
        @context[key] = val
        @workspace[key] = val
        @_with hash, fun, keys
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

  # Reset the worker state:
  #   - clear the cache
  #   - set up a new execution workspace
  #   - call the clear callback, if any
  clear: ->
    @cache = {}
    @workspace = new Worker.Workspace
    if params = @queue._params[@prefix]
      for param, i in params
        @workspace[param] = @args[i]
    Worker.clear_callback?.apply this

  # Run the body of the worker, taking its result as the value
  # of this key and passing it to `@finish`. If there is no
  # worker body defined for this (non-input) prefix, throw an
  # error.
  process: ->
    if runner = @queue._runners[@prefix]
      @finish runner.apply(@workspace, @args)
    else if @queue._is_input[@prefix]
      @finish null
    else
      throw new Error "No runner for prefix '#{prefix}'"

  # The worker is done and this is its value. Convert using `toJSON` if present,
  # set the key's value, then tell the queue that the key should be released.
  finish: (value) ->
    value = value?.toJSON?() ? value
    value = msgpack.pack value
    @kv.set @key, value, (err) =>
      @queue.finish @key
    Worker.finish_callback?.apply this
    @fiber = null

  # Convert the given error message or object into a value
  # suitable for exception bubbling, then set that error as the
  # result of this key with `@finish`.
  error: (err) ->
    trace = err.stack ? err
    error = err.get_tail?() ? [{ trace, @key, @slice }]
    @finish { error }

  # Parse the @get arguments into the prefix, key arguments,
  # and options.
  parse_args: (args) ->
    prefix = args[0]
    @on_cycle = _.callback args
    opts = _.opts args
    key = args.join consts.arg_sep
    { prefix, opts, key }

  # Inform the work queue of this dependency.
  require: (key) ->
    @queue.require key, @key

  # Tell the work queue to resume us when all the given dependencies are ready.
  # Once resumed, look up the value of all the dependencies, and resume with them.
  wait: (deps) ->
    @waiting_for = deps
    @queue.wait deps, @key

  # We have resumed after the last `@wait`, so look up the keys we're waiting on
  # and resume the fiber with them.
  stop_waiting: ->
    @kv.mget @waiting_for, (err, arr) =>
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
    if wrapper = opts.as || @queue._as[prefix]
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
      key = args.join consts.arg_sep
      prefix = args[0]
      @key_opts[key] = { prefix, opts, index }
      @all_keys.push key

  # From the keys requested by `@all`/`@each`, determine which ones are not already
  # cached locally by the worker, and store them in `@needed`; this array holds alternately
  # the lock name and the key itself, suitable for passing to `redis.mget`.
  # Also use `WorkQueue.require` to record each dependency.
  find_needed_keys: ->
    @needed = []
    for key in @all_keys
      continue if @cache[key]?
      @needed_reqs.push 'lock:'+key
      @needed_reqs.push key
      @require key

  # From the keys in `@needed`, ask the database which are available by finding both the
  # lock state and the value of the key. For any unavailable key, put that key in the `@missing`
  # list. For keys which are already available, append the key and value to the `@pending` list.
  find_missing_keys: ->
    @missing = []
    return unless @needed.length
    @kv.mget @needed, (err, arr) =>
      return @resume err if err
      i = 0
      while i < arr.length
        key = @needed[i+1]
        lock = arr[i++]
        value = arr[i++]
        lock = lock.toString() if lock
        if lock == 'ready'
          value = msgpack.unpack value
          @pending.push [key, value]
        else
          @missing.push [key, lock]
      @resume()
    @yield()

  # For each missing key, if the key is unlocked, ask the queue to enqueue that
  # key as a job. Finally, for all missing keys, yield and ask the queue to resume the
  # worker when they are available. Take the provided values from resume and put
  # them on the `@pending` list.
  get_missing_keys: ->
    return unless @missing.length
    keys = []
    for item in missing
      [key, lock] = item
      keys.push key
      @queue.enqueue key, @key, lock
    @wait keys
    for value, i in @yield()
      @pending.push [keys[i], value]

  # Keys from redis or from resuming from the queue are present in the `@pending`
  # list. For each, test the value for errors and build the key using its prefix
  # and options. If one or more values are errors, add those errors to a new
  # `MultiError` object and throw it. Make sure the context from each error key
  # is recorded on its error object. If there are no errors, return the list of values.
  build_all: ->
    multi = null
    for item in @pending
      [key, value] = value
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
