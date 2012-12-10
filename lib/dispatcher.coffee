{ exec } = require('child_process')
Doctor = require './doctor'
ControlChannel = require './control_channel'
AuditLog = require './audit_log'
RequestChannel = require './request_channel'
ResponseChannel = require './response_channel'
_ = require 'underscore'
require './util'
db = require './db'

# The dispatcher accepts requests for keys and manages the
# dependencies between jobs. It ensures that the same work
# is never requested more than once, and makes sure jobs are
# re-run whenever their dependencies are met.
class Dispatcher

  # Initializer
  constructor: (options) ->
    @deps = {}
    @link = {}
    { @max_crashes, @redis_backup_file, @redis_save_file, @recover_from_crashes } = options
    @_test_mode = options.test_mode
    @_single_use = options.single_use ? @_test_mode
    @_verbose = options.verbose
    @_idle_timeout = options.idle_timeout ? (if @_test_mode then 500 else 10000)
    @_audit_log = new AuditLog stream: options.audit
    {db_index} = options
    @_kv = db.key_value {db_index}
    @_kv.del 'jobs'
    @_kv.set "dispatcher", "reporting for duty on slice: #{db_index}"
    @_control_channel = new ControlChannel {db_index}
    @_requests_channel = new RequestChannel {db_index}
    @_responses_channel = new ResponseChannel {db_index}
    @_dependency_count = {}
    @_state = {}
    @_cycles = {}
    @_seed_count = 0
    @_seeds = {}

  connect: (callback) ->
    @_kv.connect =>
      @_control_channel.connect =>
        @_requests_channel.connect =>
          @_responses_channel.connect callback

  # Subscribe to the `requests` and `responses` channels.
  listen: ->
    @_requests_channel.listen (source, keys) => @_requested source, keys
    @_responses_channel.listen (ch, str) => @_responded str

  # Send quit signals to the work queues.
  quit: ->
    @_clear_timeout()
    @_control_channel.quit()
    finish = =>
      @_kv.end()
      @_control_channel.delete_jobs()
      @_requests_channel.end()
      @_responses_channel.end()
      @_control_channel.end()
      @_quit_handler?()
    setTimeout finish, 500

  check_for_crash: (callback) ->
    @_kv.get 'seed', (err, seed) ->
      throw err if err
      @crashed(seed) if seed
      callback()

  crashed: (seed) ->
    unless @recover_from_crashes
      @notify_crashed seed, null, false
    @_kv.redis.incr 'crash_count', (err, count) ->
      throw err if err
      if @max_crashes? && (count > @max_crashes)
        @notify_crashed seed, count, false
      @notify_crashed seed, count, true
      @save_versioned_db =>
        @_seed seed

  save_versioned_db: (callback) ->
    @_kv.redis.save (err) ->
      throw err if err
      @version_last_save()
      callback()

  version_last_save: ->
    target = @redis_backup_file + '.' + _.timestamp()
    exec "cp #{@redis_save_file} #{target}", (err) ->
      throw err if err

  notify_crashed: (seed, count, will_retry) ->
    console.log "Dispatcher detected crash:", { seed, count, will_retry, @max_crashes }
    unless will_retry
      console.log "DISPATCHER GIVING UP"
      process.exit 1

  # Provide a callback to be called when the dispatcher detects the process is stuck
  on_stuck: (@_stuck_callback) -> this

  # Set the idle handler
  on_idle: (@_idle_handler) -> this

  # Set the quit handler
  on_quit: (@_quit_handler) -> this

  # Set the quit handler
  on_quit: (@_quit_handler) -> this

  # Clear the timeout for idling
  _clear_timeout: ->
    clearTimeout @_timeout

  # Called when a worker requests keys. The keys requested are
  # recorded as dependencies, and any new key requests are
  # turned into new jobs. You can request the key `!reset` in
  # order to flush the dependency graph.
  _requested: (source, keys) ->
    if source == '!reset'
      @_reset()
    else if source == '!invalidate'
      @_invalidate key for key in keys
    else if source == '!dep'
      @_record_dep keys...
    else if source == '!dump'
      @_dump_link()
    else if source == '!replace'
      @_replace keys[0], JSON.parse(keys[1..-1].join('|'))
    else if keys?.length
      @_new_request source, keys
    else
      @_seed source

  # Store the current links in the 'deps' key
  _dump_link: (callback) ->
    @_kv.set 'deps', @link, callback

  # The given key is a 'seed' request. In test mode, completion of
  # the seed request signals termination of the workers.
  _seed: (key) ->
    @_seeds[key] = true
    @_seed_count++
    @_new_request '!seed', [key]
    @_kv.set 'seed', key

  # Forget everything we know about dependency state.
  _reset: ->
    @_dependency_count = {}
    @_state = {}
    @_cycles = {}
    @link = {}
    @deps = {}
    @doc = null
    @_control_channel.reset()

  # Invalidate the given key, then replace its value with the given string
  _replace: (key, str) ->
    @_invalidate key
    @_kv.set key, str

  # Remove the key or key-pattern from the DB and recursively invalidate its dependent keys
  _invalidate: (pattern) ->
    kill = (key) =>
      @_kv.del key
      deps = @link[key] ? []
      delete @link[key]
      delete @_state[key]
      @_remove_cycles_containing key
      @_control_channel.erase key
      kill dep for dep in deps
    if pattern.indexOf('*') >= 0
      @_kv.keys pattern, (e, keys) ->
        kill key for key in keys
    else
      kill pattern

  # Handle a request we've never seen before from a given source
  # job that depends on the given keys.
  _new_request: (source, keys) ->
    @_audit_log.request source, keys unless source == '!seed'
    @_reset_timeout()
    @_dependency_count[source] = 0
    @_handle_request source, keys

  # Reset the timer that checks if the process is broken
  _reset_timeout: ->
    @_clear_timeout()
    @_timeout = setTimeout (=> @_idle()), @_idle_timeout

  # Add an explicit dependency to `@link`
  _record_dep: (source, key) ->
    (@link[key] ?= []).push source

  # Handle the requested keys by marking them as dependencies
  # and turning any unsatisfied ones into new jobs.
  _handle_request: (source, keys) ->
    for key in _.uniq keys
      # Mark the key as a dependency of the given source job. If
      # the key is already completed, then do nothing; if it has
      # not been previously requested, create a new job for it.
      unless @_state[key] == 'done'
        @_request_dependency key unless @_state[key]?
        (@deps[key] ?= []).push source
        @_dependency_count[source]++
    unless @_dependency_count[source]
      @_reschedule source

  # Take an unmet dependency from the latest request and push
  # it onto the `jobs` queue.
  _request_dependency: (req) ->
    @_state[req] = 'wait'
    @_control_channel.push_job req

  # Signal a job to run again by sending a resume message
  _reschedule: (key) ->
    delete @_dependency_count[key]
    return @_unseed() if key == '!seed'
    return if @_state[key] == 'done'
    @_control_channel.resume key

  # The seed request was completed. In test mode, quit the workers.
  _unseed: ->
    @_kv.del 'seed'
    @_kv.del 'crash_count'
    @_dump_link =>
      unless --@_seed_count
        @_clear_timeout()
        @quit() if @_single_use

  # Called when a key is completed. Any jobs depending on this
  # key are updated, and if they have no more dependencies, are
  # signalled to run again.
  _responded: (key) ->
    @_reset_timeout()
    @_audit_log.response key
    @_state[key] = 'done'
    targets = @deps[key] ? []
    delete @deps[key]
    @_progress targets

  # Make progress on each of the given keys by decrementing
  # their count of remaining dependencies. When any reaches
  # zero, it is rescheduled.
  _progress: (keys) ->
    for key in keys
      unless --@_dependency_count[key]
        @_reschedule key

  # Activate a handler for idle timeouts. By default, this means
  # calling the doctor.
  _idle: ->
    if @_idle_handler
      @_idle_handler()
    else
      @_call_doctor()

  # Let the doctor figure out what's wrong here
  _call_doctor: ->
    console.log "Oops... calling the doctor!" if @_verbose
    @doc = new Doctor @deps, @_state, _.keys(@_seeds)
    @doc.diagnose()
    if @doc.is_stuck()
      @doc.report() if @_verbose
      @_recover()
    else
      console.log "Hmm, the doctor couldn't find anything amiss..." if @_verbose
      @_fail_recovery()

  # Recover from a stuck process.
  _recover: ->
    if @doc.recoverable()
      for cycle in @doc.cycles
        return @_fail_recovery() if @_seen_cycle cycle
      for key, deps of @doc.cycle_dependencies()
        @_signal_worker_of_cycles key, deps
      @_reset_timeout()
    else
      @_fail_recovery()

  # Determine if we've seen this cycle before
  _seen_cycle: (cycle) ->
    key = cycle.sort().join()
    return true if @_cycles[key]
    @_cycles[key] = true
    false

  # Remove any cycle that includes the given key
  _remove_cycles_containing: (key) ->
    for cycle in _.keys @_cycles
      delete @_cycles[cycle] if cycle.indexOf(key) > -1

  # Recovery failed, let the callback know about it.
  _fail_recovery: ->
    @_stuck_callback?(@doc, @_kv)

  # Tell the given worker that they have cycle dependencies.
  _signal_worker_of_cycles: (key, deps) ->
    # @_remove_dependencies key, deps
    @_control_channel.cycle key, deps

  # Remove given dependencies from the key
  _remove_dependencies: (key, deps) ->
    @_dependency_count[key] -= deps.length
    @deps[dep] = _.without @deps[dep], key for dep in deps

  # Print a debugging statement
  _debug: (args...) ->
    #console.log 'dispatcher:', args...


module.exports =

  # Create a new dispatcher instance and start it listening for
  # requests. Then return the dispatcher.
  run: (options) ->
    dispatcher = new Dispatcher(options ? {})
    dispatcher.check_for_crash ->
      dispatcher.listen()
    dispatcher
