Workspace = require './workspace'
Worker = require './worker'
events = require 'events'
consts = require './consts'
db = require './db'
util = require 'util'
msgpack = require 'msgpack'
_ = require './util'


# The `WorkQueue` accepts job requests and starts `Worker` objects
# to handle them.
class WorkQueue extends events.EventEmitter

  # Register the 'next' event, and listen for 'resume' messages.
  constructor: (@options) ->
    @queue_id = @_random_queue_id()
    @_workers = {}
    @_mixins = {}
    @_runners = {}
    @_job_queues = @options.queues ? ['jobs']
    @_kv = db.key_value @options
    @_in_queue = db.queue @options
    @_out_queue = db.queue @options
    @_pubsub = db.pub_sub @options
    @_worker_kv = db.key_value @options
    @_control_ns = _('control').namespace(@options.db_index)
    @_params = {}
    @_is_input = {}
    @_pending = []
    @_resume_for_key = {}
    @_as = {}
    @_listeners = {}
    @_task_intervals = []
    @_triggers = {}
    @_cycle_timeouts = {}
    @_queue_for_key = {}

  _random_queue_id: ->
    '' + Math.random()

  _connect: (callback) ->
    @_kv.connect =>
      @_in_queue.connect =>
        @_out_queue.connect =>
          @_pubsub.connect =>
            @_worker_kv.connect =>
              callback?()

  # Subscribe to channels and queue events
  _listen: ->
    @_pubsub.message (channel, msg) => @perform msg
    @_pubsub.subscribe @_control_ns
    @_watch_queues()

  _watch_queues: ->
    @_in_queue.redis.blpop 'dirty', @_job_queues..., 0, (err, response) =>
      return @error err if err
      [type, value] = response
      type = type.toString()
      value = value.toString()
      if type == 'dirty'
        @_handle_dirty_key value
      else
        @_handle_job_key type, value

  require: (source, target) ->
    m = @_multi()
    m.sadd "targets:#{source}", target
    m.sadd "sources:#{target}", source
    m.exec (err) => @error err if err
    @log null, 'redeye:require', { source, target }

  # Send a log message over redis pubsub
  log: (key, label, payload) ->
    return unless label and payload
    payload.key = key if key
    payload = msgpack.pack payload
    @_kv.redis.publish label, payload

  # React to a control message.
  #   ready : key was generated
  perform: (msg) ->
    msg = msg.toString()
    [action, args...] = msg.split consts.key_sep
    @_handle[action].apply this, args

  # Run the work queue, calling the given callback on completion
  run: (@callback) ->
    @_connect =>
      @_heartbeat()
      @_repeat_task 'gc', 3600, => @_garbage_collect()
      @_repeat_task 'orphan', 60, => @_check_for_orphans()
      @_listen()

  _heartbeat: ->
    @_kv.redis.setex "hb:#{@queue_id}", 10, 1
    setTimeout (=> @_heartbeat()), 5000

  # Add an accessor that @gets an input
  input: (prefix, params...) ->
    opts = _.opts params
    @_params[prefix] = params
    @_as[prefix] = opts.as
    @_is_input[prefix] = true
    Workspace.prototype[prefix] = (args...) -> @get prefix, args...

  _run: (key) ->
    try
      @log key, 'redeye:start', { @queue_id }
      @_workers[key] = new Worker key, this
      @_workers[key].run()
    catch e
      @error e

  wait: (deps, key) ->
    @_triggers[key] = deps.length
    for dep in deps
      (@_listeners[dep] ||= []).push key

  _multi: ->
    @_kv.redis.multi()

  # on pop job key
  #   record which queue for key
  #   if dirty flag
  #     enqueue key
  #   else
  #     as multi
  #       set self as owner
  #       add to set of owned jobs
  #       when done
  #         start running job
  _handle_job_key: (queue, key) ->
    @_queue_for_key[key] = queue
    if @_dirty
      @_enqueue_job queue, key
    else
      @_multi()
        .set('lock:'+key, @queue_id)
        .sadd('active:'+@queue_id, key)
        .exec (err) =>
          return @error err if err
          @_run key
    @_watch_queues()

  # on pop dirty
  #   set dirty flag
  #   delete info key
  #   look up targets
  #     push targets to dirty
  #       reset dirty timeout
  #       call popper
  _handle_dirty_key: (key) ->
    @_dirty = true
    @log key, 'redeye:dirty', {}
    @_kv.del 'lock:'+key
    @_kv.redis.smembers 'targets:'+key, (err, targets) =>
      return @error err if err
      targets = (target.toString() for target in targets)
      @_out_queue.rpush_all 'dirty', targets, (err) =>
        @error err if err
        @_reset_dirty_timeout()
        @_watch_queues()

  # The worker is asking that the given key be enqueued. We will
  # attempt to enqueue it as long as tehre is no lock. If there IS
  # already a lock on the requested key, then it is possible this
  # request will result in a cycle, so we associate a timeout with
  # the key to check for cycles.
  enqueue: (requested_key, worker_key, lock) ->
    if lock
      clearTimeout @_cycle_timeouts[requested_key]
      @_cycle_timeouts[requested_key] = setTimeout (=> @_check_for_cycles()), @_cycle_timeout
    else
      queue = @_queue_for_key[worker_key]
      @_enqueue_job queue, requested_key

  # on enqueue key
  #   acquire lock
  #     if success
  #       push key to job queue
  _enqueue_job: (queue, key) ->
    @_kv.redis.setnx 'lock:'+key, 'queued', (err, set) =>
      return @error err if err
      return unless set
      @_out_queue.lpush queue, key

  # on dirty timeout
  #   clear dirty flag
  #   take/clear all pending keys
  #   get info for pending keys
  #     for each key
  #       if missing
  #         enqueue key
  #       else
  #         next tick: call resume function
  _post_dirty: ->
    @_dirty = false
    pending = @_pending
    @_pending = []
    keys = _.map pending, (p) -> 'lock:'+p
    @_kv.redis.mget keys, (err, locks) ->
      return @error err if err
      for lock, i in locks
        lock = lock.toString() if lock
        f = @_resume_for_key keys[i]
        delete @_resume_for_key keys[i]
        if lock
          process.nextTick f
        else
          @_enqueue_job @_queue_for_key[key], key

  _reset_dirty_timeout: ->
    clearTimeout @_dirty_timeout
    @_dirty_timeout = setTimeout (=> @_post_dirty()), 1000

  _handle:

    quit: ->
      @quit()

    ready: (key) ->
      clearTimeout @_cycle_timeouts[key]
      delete @_cycle_timeouts[key]
      return unless keys = @_listeners[key]
      delete @_listeners[key]
      for key in keys
        unless --@_triggers[key]
          delete @_triggers[key]
          @_workers[key].resume()

  # Shut down the redis connection and stop running workers
  quit: ->
    @_kv.redis.del "active:#{@queue_id}", "hb:#{@queue_id}", =>
      @_kv.end()
      @_in_queue.end()
      @_out_queue.end()
      @_pubsub.end()
      @_worker_kv.end()
      @_clear_tasks()
      @callback?()

  # on job finished
  #   if dirty flag
  #     make key pending, with resume to finish job again
  #   else
  #     getset sources for key
  #       find - and + diffs
  #       as multi
  #         for each diff
  #           apply diff to target sets
  #         set state to done
  #         remove from our owned jobs
  #         on completion
  #           publish done message
  finish: (key) ->
    if @_dirty
      @_resume_for_key[key] = =>
        @finish key
      @_pending.push key
    else
      @log key, 'redeye:finish', { @queue_id }
      delete @_workers[key]
      m = @_multi()
      m.set 'lock:'+key, 'ready'
      m.srem 'active:'+@queue_id, key
      m.exec (err) =>
        return @error err if err
        @_kv.redis.publish @_control_ns, 'ready|'+key
        if key == 'fib:8' # XXX
          @quit() # XXX

  _repeat_task: (name, period, callback) ->
    interval = setInterval (=> @_lock_task name, callback, period), period*1100
    @_task_intervals.push interval

  _lock_task: (name, ttl, callback) ->
    @_kv.redis.setnx name, 1, (err, set) =>
      return @error err if err
      return unless set
      @_kv.redis.expire name, ttl
      callback()

  _garbage_collect: ->
    # add all keys to list
    # until list is empty
    #   shift key from list
    #   get sources and targets
    #   for each target
    #     look up target's sources
    #       unless key in set
    #         remove target from key's targets
    #   if not seeded, and if targets empty
    #     delete lock, key, sources, targets
    #     for each source
    #       delete key from source's targets
    #       add source to list unless exists
    # TODO

  _check_for_orphans: (callback) ->
    @_kv.redis.keys "active:*", (err, keys) =>
      return @error err if err
      keys = (key.toString() for key in keys)
      @_kv.redis.mget keys, (err, arr) =>
        return @error err if err
        bad = []
        for key, i in keys
          bad.push key unless arr[i]
        if bad.length
          @_reclaim_from bad, callback
        else
          callback()

  _reclaim_from: (active_keys, callback) ->
    m = @_multi()
    m.smembers key for key in active_keys
    m.exec (err, arrs) =>
      return @error err if err
      keys = _.flatten arrs
      @_kv.redis.del active_keys..., (err) =>
        return @error err if err
        @_kv.redis.lpush @_job_queues[0], keys..., =>
          return @error err if err
          callback()

  _check_for_cycles: ->
    return if @_checking_for_cycles
    @_checking_for_cycles = true
    @_kv.redis.keys "active:*", (err, keys) =>
      return @error err if err
      m = @_multi()
      m.smembers key for key in keys
      m.exec (err, arrs) =>
        return @error err if err
        keys = _.flatten arrs
        # TODO
        @_checking_for_cycles = false

  _clear_tasks: ->
    for interval in @_task_intervals
      clearInterval interval

  # Mark that a fatal exception occurred
  error: (err) ->
    message = err.stack ? err
    console.log "work_queue caught error:", message
    @_kv.set 'fatal', message

  # Add a worker to the context
  worker: (prefix, params..., runner) ->
    @_params[prefix] = params if params.length
    @_runners[prefix] = runner
    Workspace.prototype[prefix] = (args...) -> @get prefix, args...

  # Alias for `Worker.mixin`
  mixin: (mixins) ->
    Workspace.mixin mixins

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


module.exports = WorkQueue
