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
    @_workers = {}
    @_mixins = {}
    @_runners = {}
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
    @_as = {}
    @_listeners = {}
    @_triggers = {}
    @_cycle_timeouts = {}

  _connect: (callback) ->
    @_kv.connect =>
      @_in_ueue.connect =>
        @_out_queue.connect =>
          @_pubsub.connect =>
            @_worker_kv.connect =>
              @_worker_pubsub.connect, =>
                @_listen()
                callback?()

  # Subscribe to channels and queue events
  _listen: ->
    @_pubsub.message (channel, msg) => @perform msg
    @_pubsub.subscribe @_control_ns
    @_watch_queues()

  _watch_queues: ->
    @_in_queue.pop_any 'dirty', 'jobs', (err, response) =>
      return @error err if err
      [type, value] = response
      @_handle[type] value

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
    @_connect()

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
      @_kv.del "sources:#{key}", "targets:#{key}"
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

  _handle:

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

  finish_key: (key, state) ->
    @_kv.get "info:#{key}", (err, info) =>
      return @error err if err
      if info.state == 'working'
        @_kv.set "info:#{key}", {state}, (err) ->
          @error err if err
          @_pubsub.publish @_control_ns, "ready|#{key}"

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
