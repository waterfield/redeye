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
    @resume.on 'message', (channel, key) =>
      @workers[key]?.resume()
    @resume.subscribe "resume_#{@options.db_index}"
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
      throw err if err
      return @quit() if str == '!quit'
      @workers[str] = new Worker(str, this, @sticky)
      @workers[str].run()
      @emit 'next'
  
  # Shut down the redis connection and stop running workers
  quit: ->
    @db.end()
    @resume.end()
    @worker_db.end()
    @callback?()


# The worker class is the context under which runner functions are run.
class Worker

  # Find the runner for the `@key`. The key is in the format:
  # 
  #     prefix:arg1:arg2:...
  constructor: (@key, @queue, @sticky) ->
    [@prefix, args...] = @key.split consts.arg_sep
    @args = args # weird bug in coffeescript: wanted @args... in line above
    @db = @queue.worker_db
    @req_channel = "requests_#{@queue.options.db_index}"
    @resp_channel = "responses_#{@queue.options.db_index}"
    unless @runner = @queue.runners[@prefix]
      throw new Error("no runner for '#{@prefix}' (#{@key})")
    @cache = {}
    @last_stage = 0
    num_workers++

  # If we've already seen this `@get` before, then return the actual
  # value we've received (which we know we got because otherwise we
  # wouldn't be running again). Otherwise, just mark this dependency
  # and return `undefined`.
  get: (args...) ->
    opts = @opts args
    key = args.join consts.arg_sep
    if @sticky[key]
      @sticky[key]
    else if @stage < @last_stage
      value = @build @cache[key], opts.as
      @sticky[key] = value if opts.sticky
      value
    else
      @deps.push key
      undefined
  
  # This is a bit of syntactic sugar. It's the equivalent of:
  # 
  #     x = @get key
  #     @for_reals()
  #     x
  get_now: (args...) ->
    value = @get args...
    @for_reals()
    value

  # Extract an optional options hash from a list of arguments
  opts: (args) ->
    if typeof(args[args.length-1]) == 'object'
      args.pop()
    else
      {}
  
  # If a klass is given, construct a new one; otherwise, just return
  # the raw value.
  build: (value, klass) ->
    if klass? then @bless(new klass(value)) else value
  
  # Extend the given object with the context methods of a worker,
  # in addition to a recursive blessing.
  bless: (object) ->
    me = this
    for method in ['get', 'emit', 'for_reals', 'get_now']
      do (method) -> object[method] = (args...) => me[method].apply me, args
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
      throw 'resolve' if @deps.length
      @last_stage++
    @stage++

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
  clear: ->
    @stage = 0
    @deps = []
    @emitted = false

  # If the caught error is from a `@for_reals`, then try to resolve
  # dependencies.
  caught: (err) ->
    if err == 'resolve'
      @resolve()
    else
      throw err

  # Call the runner. If it gets all the way through, first check if there
  # were any unmet dependencies after the last `@for_reals`. If so, we force
  # one last dependency resolution. Otherwise, we optionally
  # emit the result of the function (if nothing has been emitted yet).
  process: ->
    result = @runner.apply this, @args
    return @resolve() if @deps.length
    num_workers--
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
    @last_stage++
    @get_deps()

  # Ask redis to provide values for our dependencies. If any are missing,
  # send a request to the dispatcher; otherwise, resume trying to run the
  # main function.
  get_deps: ->
    @db.mget @deps, (err, arr) =>
      throw err if err
      bad = @check_values arr
      if bad.length
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
    @get_deps()


module.exports =
  
  # Create and return a new work queue with the given options.
  queue: (options) -> new WorkQueue(options ? {})
