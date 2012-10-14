Workspace = require './workspace'
Worker = require './worker'
events = require 'events'
consts = require './consts'
db = require './db'
_ = require 'underscore'
require './util'
util = require 'util'
msgpack = require 'msgpack'

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
    @_worker_pubsub = db.pub_sub @options
    @_control_ns = _('control').namespace(@options.db_index)
    @_worker_count = 0
    @_params = {}
    @_is_input = {}
    @_pending = []
    @_resume_for_key = {}
    @_as = {}
    @_listeners = {}
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
              @_worker_pubsub.connect, =>
                callback?()

  # Subscribe to channels and queue events
  _listen: ->
    @_pubsub.message (channel, msg) => @perform msg
    @_pubsub.subscribe @_control_ns
    @_watch_queues()

  _watch_queues: ->
    @_in_queue.pop_any 'dirty', @_job_queues..., (err, response) =>
      return @error err if err
      [type, value] = response
      if type == 'dirty'
        @_handle_dirty_key value
      else
        @_handle_job_key type, value

  # cleanup procedure
  #   find keys which are done, but which have no targets
  #     if none, return
  #     delete them and their state
  #     get/delete their sources
  #     remove key from each source's target
  #   repeat
  cleanup: ->
    # TODO

  # Send a log message over redis pubsub
  log: (key, label, payload) ->
    return unless label and payload
    payload.key = key
    # payload = JSON.stringify payload
    payload = msgpack.pack payload
    @_worker_pubsub.publish label, payload

  # React to a control message from another queue.
  #   ready : key was generated
  perform: (msg) ->
    [action, args...] = msg.split consts.key_sep
    @_handle[action].apply this, args

  # Run the work queue, calling the given callback on completion
  run: (@callback) ->
    @_connect =>
      @_kv.get '_dirty', (err, val) =>
        return @error err if err
        @perform 'dirty' if val
        @_listen()

  # Add an accessor that @gets an input
  input: (prefix, params...) ->
    opts = _.opts params
    @_params[prefix] = params
    @_as[prefix] = opts.as
    @_is_input[prefix] = true
    Workspace.prototype[prefix] = (args...) -> @get prefix, args...

  params_for: (prefix) ->
    @_params[prefix]

  # Look for the next job using BLPOP on the "jobs" queue. This
  # will use an event emitter to call `next` again, so the stack
  # doesn't get large.
  #
  # You can push the job `!quit` to make the work queue die.
  next: ->
    @_queue.pop 'jobs', (err, str) =>
      if err
        @emit 'next'
        return @error err
      try
        @_worker_count++
        @log str, 'redeye:start', {}
        @workers[str] = new Worker(str, this, @sticky)
        @workers[str].run()
        # console.log @_worker_count
      catch e
        @error e unless e == 'no_runner'
      @emit 'next'

  _run: (key) ->
    try
      @_worker_count++
      @_workers[key] = new Worker key, this
      @_workers[key].run()
    catch e
      @error e unless e == 'no_runner'

  listen_for: (deps, key) ->
    wait = []
    for dep in deps
      unless @_readied[key]
        wait.push dep
    if wait.length
      @_triggers[key] = wait.length
      for dep in wait
        (@_listeners[dep] ||= []).push key
    else
      @_workers[key].resume()

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
        .set('_lock:'+key, @queue_id)
        .sadd('_active:'+@queue_id, key)
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
    @_kv.del '_lock:'+key
    @_kv.smembers '_targets:'+key, (err, targets) =>
      return @error err if err
      @_out_queue.rpush_all 'dirty', targets, (err) =>
        @error err if err
        @_reset_dirty_timeout()
        @_watch_queues()

  # on enqueue key
  #   acquire lock
  #     if success
  #       push key to job queue
  _enqueue_job: (queue, key) ->
    @_kv.setnx '_lock:'+key, 'queued', (err, set) =>
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
    @_kv.del '_dirty'
    pending = @_pending
    @_pending = []
    keys = _.map pending, (p) -> '_lock:'+p
    @_kv.mget keys, (err, locks) ->
      return @error err if err
      for lock, i in locks
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

    dirty: ->
      @_dirty = true
      @_reset_dirty_timeout()

    ready: (key) ->
      clearTimeout @_cycle_timeouts[key]
      if keys = @_listeners[key]
        delete @_listeners[key]
        for key in keys
          unless --@_triggers[id]
            delete @_triggers[id]
            @_workers[key].resume()
      else
        @_readied[key] = true

    jobs: (key) ->
      @_kv.setnx key, {state: 'working'}, (err, set) ->
        @_run key if set
        @_watch_queues()

    dirty: (key) ->
      @_kv.set "info:#{key}", state: 'dirty'
      @_kv.smembers "targets:#{key}", (err, targets) =>
        return @error err if err
        @_out_queue.lpush_all 'dirty', targets, (err) =>
          @error err if err
          @_watch_queues()

  # Shut down the redis connection and stop running workers
  quit: ->
    @_kv.end()
    @_in_queue.end()
    @_out_queue.end()
    @_pubsub.end()
    @_worker_kv.end()
    @_worker_pubsub.end()
    @callback?()

  # Mark the given worker as finished (release its memory)
  finish_worker: (key) ->
    @log key, 'redeye:finish', {}
    @_worker_count--
    delete @_workers[key]

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
  finish_key: (key) ->
    if @_dirty
      @_resume_for_key[key] = =>
        @finish_key key
      @_pending.push key
    else
      new_sources = @_workers[key].dependencies
      @_kv.getset '_sources:'+key, new_sources, (err, old_sources) =>
        return @error err if err
        [adds, dels] = @_array_diff old_sources, new_sources
        m = @_multi()
        for source in dels
          m.sdel '_targets:'+source, key
        for source in adds
          m.sadd '_targets:'+source, key
        m.set '_lock:'+key, 'done'
        m.sdel '_active:'+@queue_id, key
        m.exec (err) =>
          return @error err if err
          @_pubsub.publish @_control_ns, 'ready|'+key

  _array_diff: (a, b) ->
    a = _.clone(a).sort()
    b = _.clone(b).sort()
    add = []
    del = []
    while a.length && b.length
      if a[0] == b[0]
        a.shift(); b.shift()
      else if a[0] < b[0]
        del.push a.shift()
      else
        add.push b.shift()
    [add.concat(b), del.concat(a)]

  watch_for_cycle: (key) ->
    @_cycle_timeouts[key] = setTimeout (=> @_check_for_cycle key), @_cycle_timeout

  _check_for_cycle: (key) ->
    # TODO

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

  params_for: (prefix) ->
    @_params[prefix]

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
