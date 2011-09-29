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
consts = require('./consts')
db = require('./db')()
events = require 'events'

# Counts the number of simultaneous workers.
num_workers = 0

# This holds a list of registered runners, hashed by their job prefix.
#
# For instance:
#
#     runners =
#       rand: (n) -> ...
#       add: (a,b) -> ...
runners = {}

# The `WorkQueue` accepts job requests and starts `Worker` objects
# to handle them.
# 
# FIXME: the BLPOP could potentially cause an unbalanced
# acquisition of jobs, where all but one worker are starved.
class WorkQueue extends events.EventEmitter

  # Register the 'next' event.
  initialize: ->
    @on 'next', => @next()

  # Look for the next job using BLPOP on the "jobs" queue. This
  # will use an event emitter to call `next` again, so the stack
  # doesn't get large.
  # 
  # You can push the job `!quit` to make the work queue die.
  next: ->
    db.blpop 'jobs', (err, str) ->
      throw err if err
      return if str == '!quit'
      new Worker(str).run =>
        @emit 'next'


# The worker class is the context under which runner functions are run.
class Worker

  # Find the runner for the `@key`. The key is in the format:
  # 
  #     prefix:arg1:arg2:...
  initialize: (@key) ->
    [@prefix, @args...] = @key.split consts.arg_sep
    @runner = runners[@prefix]
    throw "no runner for '#{prefix}'" unless @runner
    @cache = {}
    @last_stage = 0
    num_workers++

  # If we've already seen this `@get` before, then return the actual
  # value we've received (which we know we got because otherwise we
  # wouldn't be running again). Otherwise, just mark this dependency
  # and return `undefined`.
  get: (key) ->
    if @stage < @last_stage
      @cache[key]
    else
      @deps.push key
      undefined

  # Produce `value` as a result for `key`. This both puts the result
  # in redis under the key and tells the dispatcher (via the `responses`
  # channel) that the job is done.
  emit: (key, value) ->
    @emitted = true
    db.set key, JSON.stringify(value)
    db.publish 'responses', key

  # If we've seen this `@for_reals` before, then blow right past it.
  # Otherwise, abort the runner function and start over (after checking
  # that our dependencies are met).
  for_reals: ->
    if @stage == @last_stage
      throw 'resolve'
    else
      @stage++

  # Attempt to run the runner function. If a call to `@for_reals` causes
  # us to abort, then attempt to resolve the dependencies.
  run: (callback) ->
    try
      @clear()
      @process()
    catch err
      @caught err
    finally
      callback()

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

  # Call the runner. If it gets all the way through, then optionally
  # emit the result of the function (if nothing has been emitted yet).
  process: ->
    result = @runner.call this, @args
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
    db.mget @deps, (err, arr) ->
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
    db.publish 'requests', request
    lock = "resume_#{@key}"
    db.blpop lock, (err, _) =>
      throw err if err
      @get_deps()


# Export the `runners` and a main `run` function that kicks
# off the `WorkQueue`.
module.exports =

  runners: runners
  
  run: ->
    new WorkQueue().next()
