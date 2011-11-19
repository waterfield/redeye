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
    @worker_db = db @options.db_index
    @workers = {}
    @runners = {}
    @sticky = {}
    @mixins = {}
    @resume.on 'message', (channel, key) =>
      @workers[key]?.resume()
    @resume.subscribe _('resume').namespace(@options.db_index)
    @on 'next', => @next()
  
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
      return @quit() if str == '!quit'
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
    @worker_db.end()
    @callback?()
    
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
    if @sticky[key]
      @sticky[key]
    else if @stage < @last_stage
      # TODO: move the @build and @sticky[]= into @get_deps
      value = @build @cache[key], opts.as
      @sticky[key] = value if opts.sticky
      value
    else
      @deps.push key
      undefined
  
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
#    console.log 'current:', Worker.current # XXX
    value = @get.apply this, arguments
    @for_reals()
    value

  # If a klass is given, construct a new one; otherwise, just return
  # the raw value.
  build: (value, klass) ->
    if klass? then @bless(new klass(value)) else value
  
  # Extend the given object with the context methods of a worker,
  # in addition to a recursive blessing.
  bless: (object) ->
    for method in ['get', 'emit', 'for_reals', 'get_now', 'keys']
      do (method) -> object[method] = ->
        Worker.current[method].apply Worker.current, arguments
    object.bless = (next) => @bless next
    object

  # Produce `value` as a result for `key`. This both puts the result
  # in redis under the key and tells the dispatcher (via the `responses`
  # channel) that the job is done.
  emit: (args..., value) ->
    @emitted = true
    key = args.join consts.arg_sep
    @db.set key, JSON.stringify(value)
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
    @deps = []
    @emitted = false
    @search = null
    @is_async = false

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
    num_workers--
    @queue.finish @key
    if result? && !@emitted
      @emit @key, result

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


# Extend the blessed methods with the given ones, so that
# worker contexts can use them.
Worker.mixin = (mixins) ->
  for name, fun of mixins
    do (fun) ->
      Worker.prototype[name] = ->
        fun.apply Worker.current, arguments

module.exports =
  
  # Create and return a new work queue with the given options.
  queue: (options) -> new WorkQueue(options ? {})
