# Red Eye
# =======
# 
# Fast parallel workers
# ---------------------
#
# Red eye workers handle a single job until completion. The runner defined
# by the prefix of the job contains the code to perform the computation.
# The runner uses the context of the worker, and has access to three important
# methods:
# 
# * `@get(key)`: returns named key from the database
# * `@emit(key, value)`: stores value for named key to the database
# * `@for_reals()`: stop and make sure all dependencies have been met
# 
# The first time a key is requested with `@get`, the true value of that
# key is not returned. Only after calling `@for_reals()` are those variables
# substituted with their actual values. In order to minimize total latency,
# you should use as few calls to `@for_reals` as possible, but remember that
# the values returned by `@get` aren't useful until then.
# 
# The runner function is called with the arguments of the job. It can either
# use `@emit` to indicate its result(s), or it can simply return a single
# result from the function, but not both.

# Dependencies.
events = require 'events'
consts = require './consts'
db = require './db'
_ = require 'underscore'
require './util'

# Counts the number of simultaneous workers.
num_workers = 0

# The `WorkQueue` accepts job requests and starts `Worker` objects
# to handle them.
class WorkQueue extends events.EventEmitter

  # Register the 'next' event, and listen for 'resume' messages.
  constructor: (@options) ->
    @db = db @options.db_index
    @resume = db @options.db_index
    @control = db @options.db_index
    @worker_db = db @options.db_index
    @workers = {}
    @runners = {}
    @sticky = {}
    @mixins = {}
    @listen()
    @on 'next', => @next()
  
  # Subscribe to channels
  listen: ->
    @resume.on 'message', (channel, key) =>
      @workers[key]?.resume()
    @resume.subscribe _('resume').namespace(@options.db_index)

    @control.on 'message', (channel, msg) => @perform msg
    @control.subscribe _('control').namespace(@options.db_index)
  
  # React to a control message sent by the dispatcher
  perform: (msg) ->
    action, args... = msg.split consts.key_sep
    switch action
      when 'quit' then @quit()
      when 'reset' then @reset()
      when 'cycle' then @cycle_detected args...
  
  # The dispatcher is telling us the given key is part of a cycle. If it's one
  # of ours, cause the worker to re-run, but throwing an error from the @get that
  # caused the cycle. On the plus side, we can assume that all the worker's non-
  # cycled dependencies have been met now.
  cycle_detected: (key, dependencies...) ->
    if worker = @workers[key]
      for dep in dependencies
        worker.cycle[dependency] = true
  
  # Run the work queue, calling the given callback on completion
  run: (@callback) ->
    @next()  
    
  # Add a worker to the context
  worker: (prefix, runner) ->
    @runners[prefix] = runner

  # Look for the next job using BLPOP on the "jobs" queue. This
  # will use an event emitter to call `next` again, so the stack
  # doesn't get large.
  # 
  # You can push the job `!quit` to make the work queue die.
  next: ->
    @db.blpop 'jobs', 0, (err, [key, str]) =>
      if err
        @emit 'next'
        return @error err
      try
        @workers[str] = new Worker(str, this, @sticky)
        @workers[str].run()
      catch e
        @error e unless e == 'no_runner'
      @emit 'next'
  
  # Shut down the redis connection and stop running workers
  quit: ->
    @db.end()
    @resume.end()
    @control.end()
    @worker_db.end()
    @callback?()
  
  # Clean out the sticky cache
  reset: ->
    console.log 'worker resetting!' # XXX
    @sticky = {}
    
  # Mark the given worker as finished (release its memory)
  finish: (key) ->
    delete @workers[key]
  
  # Mark that a fatal exception occurred
  error: (err) ->
    message = err.stack ? err
    console.log message
    @db.set 'fatal', message
  
  # Print a debugging statement
  debug: (args...) ->
    #console.log 'queue:', args...
  
  # Alias for `Worker.mixin`
  mixin: (mixins) ->
    Worker.mixin mixins
  
  # Provide a callback to be executed in the context
  # of a worker whenever it has finished running, but before
  # saving its resutlts
  on_finish: (callback) ->
    Worker.finish_callback = callback
    this
  
  # Provide a callback to be called every time the worker begings running
  on_clear: (callback) ->
    Worker.clear_callback = callback
    this


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
    else if @stage < @last_stage
      value = @build @cache[key], opts.as
      @sticky[key] = value if opts.sticky
      value
    else if @cycle[key]
      @cycle[key] = false
      throw new CycleError
    else
      @deps.push key
      @blank()
  
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
    @cycle = {}
    Worker.clear_callback?.apply this

  # If the caught error is from a `@for_reals`, then try to resolve
  # dependencies.
  caught: (err) ->
    if err == 'resolve'
      @resolve()
    else
      @error err

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

  # The dispatcher said to resume, so go look for the missing values again.
  resume: ->
    @get_deps true
  
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

module.exports =
  
  # Create and return a new work queue with the given options.
  queue: (options) -> new WorkQueue(options ? {})
