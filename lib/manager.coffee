msgpack = require 'msgpack'
uuid = require 'node-uuid'
{EventEmitter2} = require 'eventemitter2'
{CycleError} = require './errors'
Workspace = require './workspace'
Worker = require './worker'
Cache = require './cache'
pool = require './pool'
util = require 'util'
scripts = require './scripts'
stats = require './stats'
_ = require './util'

# The manager creates and handles `Worker` objects.
class Manager extends EventEmitter2

  constructor: (opts = {}) ->
    @id = uuid.v1()
    @workers = {}
    @mixins = {}
    @runners = {}
    @queues = opts.queues ? ['jobs']
    @max_cache_items = opts.max_cache_items || 100
    @params = {}
    { @verbose, @flush, @slice, @host, @port } = opts
    @control = if @slice then "control_#{@slice}" else 'control'
    @opts = {}
    @done = {}
    @listeners = {}
    @triggers = {}
    @helpers = {}
    @helper_values = {}
    @task_intervals = []
    @cache = new Cache max_items: @max_cache_items
    # @diag_interval = setInterval (=> console.log @diagnostic()), 5000 # we will handle orphans differently in the future.

  # UTILITY METHODS
  # ===============

  # Print information about the state of each key
  diagnostic: ->
    log = ['Manager: ', @id, "\n"]
    for key, worker of @workers
      if worker.waiting_for
        log.push ['  [waiting] ', key, "\n"]
        for dep in worker.waiting_for
          code = if @done[dep] then 'D' else if @workers[dep] then 'A' else 'M'
          log.push ['    ', code, ' ', dep, "\n"]
      else
        log.push ['  [running] ', key, "\n"]
    _.flatten(log).join('')

  # Reset the cache and list of done keys
  reset: ->
    @cache.reset()
    @done = {}

  # API METHODS
  # ===========

  # Start running the manager. When the manager exits,
  # the callback, if provided, will be called.
  run: (@callback) ->
    @connect (err) =>
      throw err if err
      finish = =>
        # @repeat_task 'orphan', 10, => @check_for_orphans()
        @heartbeat()
        @listen()
        @emit 'ready'
      if @flush
        @db.flushdb -> finish()
      else
        finish()

  # Add a worker declaration to this manager. Declarations look like this:
  #
  #     m.worker 'prefix', 'param1', 'param2', (arg1, arg2) -> body...
  #
  # The parameters or arguments are optional. It is recommended that
  # you provide parameters. In this case, rather than using arguemnts,
  # the parameters are converted into instance variables (`@param` etc.)
  # for you in the context of the workspace.
  #
  # By registering the worker, you also create a workspace API method
  # of the same name as the worker prefix, which is a shortcut for
  # `@get(prefix, ...)`.
  worker: (prefix, params..., runner) ->
    opts = _.opts params
    opts.namespace = @default_namespace if opts.namespace == undefined
    short_prefix = prefix
    prefix = "#{opts.namespace}.#{prefix}" if opts.namespace
    @params[prefix] = params if params.length
    @opts[prefix] = opts
    @runners[prefix] = runner
    @helpers[prefix] = true if opts.helper
    Workspace.prototype[short_prefix] = (args...) -> @get short_prefix, args...

  # Declare a number of workers, all in a given namespace
  namespace: (namespace, body) ->
    @default_namespace = namespace
    body()
    @default_namespace = undefined

  # XXX XXX XXX
  input: (args...) ->
    @worker args..., -> null

  # Mix-in some external methods into the Workspace API.
  mixin: (mixins) ->
    Workspace.mixin mixins

  # Request a key from nowhere, as a seed.
  request: (key, queue, callback) ->
    if typeof(queue) == 'function'
      callback = queue
      queue = null
    queue ||= @queues[0]
    @require queue, [key], null, (err, values) =>
      value = values[0] unless err
      callback?(err, value)

  check_helpers: (key) ->
    [prefix, args...] = key.split(':')
    return undefined unless @helpers[prefix]
    value = @helper_values[key]
    return value if value != undefined
    @helper_values[key] = @run_helper(prefix, args)

  # WORKER-API METHODS
  # ==================

  run_helper: (prefix, args) ->
    _.standardize_args args
    workspace = new Worker.Workspace
    @runners[prefix].apply workspace, args

  # Check our LRU cache to see if the given key is in it.
  check_cache: (key) ->
    @cache.get key

  # Add an item to the LRU cache
  add_to_cache: (prefix, key, value, sticky) ->
    sticky ||= @opts[prefix]?.sticky
    @cache.add key, value, sticky

  # Log that a dependency is being removed (because on a re-run of a dirty
  # key, a prior dependency was dropped).
  unrequire: (source, target) ->
    @log null, 'redeye:unrequire', { source, target }

  # The given target key is requesting these sources. Use `request.lua` to
  # try to satisfy these dependencies. If the script indicates a cycle, insert
  # a cycle error into the requesting worker's lifecycle. Otherwise, return
  # the value(s) returned by the script, knowing that some of them may be
  # `null`, so the worker will then call `@wait` on us.
  require: (queue, sources, target, callback) ->
    @db.evalsha @scripts.require, 0, queue, target, sources..., (err, arr) =>
      return @error err if err
      if arr.shift().toString() == 'cycle'
        return callback(new CycleError [arr..., target])
      values = for buf in arr
        msgpack.unpack(buf) if buf
      for source in sources
        @log null, 'redeye:require', { source, target }
      callback null, values

  # Log a message. Each message type has its own channel.
  log: (key, label, payload) ->
    return unless label and payload
    payload.key = key if key
    payload.slice = @slice if @slice
    console.log new Date().getTime(), @slice, label, payload if @verbose
    @emit label, payload
    payload = msgpack.pack payload
    @db.publish label, payload
    stats.increment "events.#{label.split(':').join('.')}"

  # Set up listeners for the given dependencies, such that when all
  # of them are satisfied, the worker for the key is resumed.
  wait: (deps, key) ->
    @triggers[key] = deps.length
    for dep in deps
      (@listeners[dep] ||= []).push key
      @handle.ready.apply @, [dep] if @done[dep]

  # A worker has finished with the given value, so use `finish.lua` to
  # attempt to wrap up the worker.
  finish: (id, key, value, callback) ->
    @db.evalsha @scripts.finish, 0, @control, id, @id, key, value, (err, set) =>
      delete @workers[key]
      @log(key, 'redeye:finish', {}) if set
      callback set

  # INTERNAL METHODS
  # ================

  # Load lua scripts into redis, and acquire our three database connections:
  #
  # * `@pop`: used to continually call `blpop` on the work queues
  # * `@sub`: used to listen for control messages
  # * `@db`: used for everything else
  connect: (callback) ->
    @pool = pool({@slice, @port, @host})
    @pool.acquire (err, @pop) =>
      return callback(err) if err
      @pool.acquire (err, @sub) =>
        return callback(err) if err
        @pool.acquire (err, @db) =>
          return callback(err) if err
          scripts.load @db, (err, @scripts) =>
            callback err

  # Start listening on the control channel, calling `@perform` when each
  # message is received.
  listen: ->
    @sub.on 'message', (channel, msg) => @perform msg
    @sub.subscribe @control
    @safely_pop_next()

  # Emit an event to pop the next job from the queue, so we don't
  # blow up the stack
  safely_pop_next: ->
    return if @popping
    @popping = true
    process.nextTick =>
      @popping = false
      @pop_next()

  # Pop job message from the work queues and call `@job` to handle it;
  # some time later `@pop_next` will be called again.
  pop_next: ->
    @pop.blpop @queues..., 0, (err, response) =>
      return @error err if err
      [type, value] = response
      @job type, value

  # Dispatch the control message to one of our handlers.
  perform: (msg) ->
    [action, args...] = msg.toString().split '|'
    @handle[action].apply this, args

  # Repeatedly set a heartbeat key in redis, that will expire automatically
  # if this manager dies for some reason. That way orphans in our active set
  # can be freed by some later manager and `orphans.lua`.
  heartbeat: ->
    @db.setex "heartbeat:#{@id}", 10, 1
    @heartbeat_timeout = setTimeout (=> @heartbeat()), 5000

  # We received a key from the work queue; claim that key by putting it into
  # our working set, changing the key lock to be a unique worker key, and removing
  # it from the pending set. Also look up then erase the key's sources, so we
  # can do source-target garbage collection when the key is complete.
  claim: (key, callback) ->
    id = uuid.v1()
    @db.multi()
      .smembers('sources:'+key)
      .del('sources:'+key)
      .set('lock:'+key, id)
      .sadd('active:'+@id, key)
      .srem('pending', key)
      .exec (err, arr) =>
        return @error err if err
        deps = if arr[0].length then arr[0].split(',') else []
        callback id, deps

  # First `@claim` the key, then start a worker for the key and tell it to
  # run. Then, go back and wait for the next job on the work queues.
  job: (queue, key) ->
    @claim key, (id, sources) =>
      try
        @workers[key] = new Worker id, key, queue, sources, this
        @workers[key].run()
      catch e
        @error e
      @safely_pop_next()

  # We want the given worker to resume running, either because its
  # dependencies have been satisfied, or we want to inject an error
  # into its fiber, or we want to mark it as dirty and let it implode
  # from within.
  resume: (key, dirty, err) ->
    return unless worker = @workers[key]
    worker.dirty = true if dirty
    worker.resume(err)

  # Gracefully shut down, by
  #
  # * deleting our heartbeat
  # * clearing repeated tasks
  # * imploding all workers
  # * re-enqueing all active keys
  # * moving active keys back to pending set
  # * draining the db pool
  quit: ->
    return if @_quit
    @_quit = true
    @clear_tasks()
    clearTimeout @heartbeat_timeout
    clearInterval @diag_interval
    m = @db.multi()
    m.del 'heartbeat:'+@id
    for key, worker of @workers
      m.srem('active:'+@id, key)
       .sadd('pending', key)
       .lpush(worker.queue, key)
      worker.implode()
    @workers = null
    m.exec (err) =>
      return @error err if err
      @pool.release(@db); @db = null
      @pool.release(@sub); @sub = null
      @pool.release(@pop); @pop = null
      @pool.drain =>
        @pool.destroyAllNow =>
          @emit 'quit'
          @callback?()
          @callback = null

  handle:

    # We received a quit message, so exit gracefully;
    quit: -> @quit()

    # The given key has been marked as dirty by the `dirty.lua` script.
    # Mark the worker as dirty so that when it resumes, for any reason,
    # it will implode itself. Then, call `@ready` so that any workers
    # listening for the given key will mark themselves as dirty and
    # recursively implode as well.
    dirty: (key) ->
      if worker = @workers[key]
        worker.dirty = true
        @db.srem 'active:'+@id, key
        @remove_worker key
      @handle.ready.apply @, [key, true]

    # A key was completed. If we have any listeners for that key,
    # decrement their trigger count. If they have no remaining listeners,
    # resume that key. It will then grab its dependency values from
    # the database and resume work.
    ready: (key, dirty) ->
      @done[key] = true unless dirty
      return unless keys = @listeners[key]
      delete @listeners[key]
      for key in keys
        unless @triggers[key] && (--@triggers[key])
          delete @triggers[key]
          @resume key, dirty

  # Remove the given key from our memory (it is  still up to the worker
  # to make sure internal references are erased). If the `with_prejudice`
  # flag is provided, eliminate all trace of the worker from the database
  # as well.
  remove_worker: (key, with_prejudice) ->
    return unless worker = @workers[key]
    delete @triggers[key]
    delete @workers[key]
    if with_prejudice
      @db.multi()
        .del('lock:'+key)
        .del('sources:'+key)
        .del('targets:'+key)
        .srem('active:'+@id, key)
        .exec (err) =>
          @error err if err

  # Set the given callback as a task to repeat once per period. Since there
  # may be multiple managers with the same task, make sure a lock is acquired
  # on the task so that it happens once between every period and 1.1*period.
  repeat_task: (name, period, callback) ->
    interval = setInterval (=> @lock_task name, period, callback), period*1100
    @task_intervals.push interval

  # Attempt to lock the given task; if the lock was acquired, run the callback.
  # Then expire the task so it can be run again next period.
  lock_task: (name, ttl, callback) ->
    @db.setnx 'task:'+name, 1, (err, set) =>
      return @error err if err
      return unless set
      @db.expire 'task:'+name, ttl
      callback()

  # One of the repeatable tasks. Call the `orphans.lua` script to see if a
  # worker has died and left orphans. Then brag about it on the console if
  # we saved any.
  check_for_orphans: ->
    @db.evalsha @scripts.orphans, 0, @queues[0], (err, num) =>
      return @error err if err
      console.log "#{@id} rescued #{num} orphans" if num

  # Stop all repeated tasks by deleting their intervals.
  clear_tasks: ->
    for interval in @task_intervals
      clearInterval interval

  # Mark that a fatal exception occurred
  error: (err) ->
    return unless err
    message = err.stack ? err
    @db.set 'fatal', message
    console.log message
    @quit()


module.exports = Manager
