consts = require './consts'
Doctor = require './doctor'
db = require('./db')
_ = require 'underscore'
require './util'

# The dispatcher accepts requests for keys and manages the
# dependencies between jobs. It ensures that the same work
# is never requested more than once, and makes sure jobs are
# re-run whenever their dependencies are met.
class Dispatcher

  # Initializer
  constructor: (@options) ->
    @test_mode = @options.test_mode
    @verbose = @options.verbose
    @idle_timeout = @options.idle_timeout ? (if @test_mode then 500 else 10000)
    @audit_stream = @options.audit
    @db = db @options.db_index
    @req = db @options.db_index
    @res = db @options.db_index
    @control_channel = _('control').namespace @options.db_index
    @count = {}
    @state = {}
    @deps = {}
    @cycle_keys = {}
    @unmet = 0

  # Subscribe to the `requests` and `responses` channels.
  listen: ->
    @req.on 'message', (ch, str) => @requested str
    @res.on 'message', (ch, str) => @responded str
    @req.subscribe _('requests').namespace(@options.db_index)
    @res.subscribe _('responses').namespace(@options.db_index)
  
  # Called when a worker requests keys. The keys requested are
  # recorded as dependencies, and any new key requests are
  # turned into new jobs. You can request the key `!reset` in
  # order to flush the dependency graph.
  requested: (str) ->
    [source, keys...] = str.split consts.key_sep
    if keys.length
      @audit "?#{str}"
      @new_request source, keys
    else if source == '!reset'
      @reset()
    else
      @seed source
  
  # Determine if we're still busy recovering from a cyclic dependency. This will be
  # true if there are currently any unresolved cycle keys.
  recovering: ->
    _.keys(@cycle_keys).length > 0
  
  # Forget everything we know about dependency state.
  reset: ->
    @count = {}
    @state = {}
    @deps = {}
    @db.publish @control_channel, 'reset'
  
  # Print a debugging statement
  debug: (args...) ->
    #console.log 'dispatcher:', args...
  
  # Called when a key is completed. Any jobs depending on this
  # key are updated, and if they have no more dependencies, are
  # signalled to run again.
  responded: (key) ->
    @audit "!#{key}"
    @state[key] = 'done'
    targets = @deps[key] ? []
    delete @deps[key]
    delete @cycle_keys[key]
    @progress targets

  # Write text to the audit stream
  audit: (text) ->
    console.log text # XXX
    @audit_stream.write "#{text}\n" if @audit_stream

  # The given key is a 'seed' request. In test mode, completion of
  # the seed request signals termination of the workers.
  seed: (key) ->
    @_seed = key
    @new_request '!seed', [key]
  
  # The seed request was completed. In test mode, quit the workers.
  unseed: ->
    console.log 'unseeding!' # XXX
    @clear_timeout()
    @quit() if @test_mode
  
  # Send quit signals to the work queues.
  quit: ->
    @clear_timeout()
    @db.publish @control_channel, 'quit'
    finish = =>
      @db.del 'jobs'
      @req.end()
      @res.end()
      @db.end()
    setTimeout finish, 500

  # Make progress on each of the given keys by decrementing
  # their count of remaining dependencies. When any reaches
  # zero, it is rescheduled.
  progress: (keys) ->
    for key in keys
      @unmet--
      unless --@count[key]
        @reschedule key
  
  # Set the idle handler
  on_idle: (@idle_handler) -> this
  
  # Clear the timeout for idling
  clear_timeout: ->
    clearTimeout @timeout

  # Reset the timer that checks if the process is broken
  reset_timeout: ->
    @clear_timeout()
    @timeout = setTimeout (=> @idle()), @idle_timeout
  
  # Activate a handler for idle timeouts. By default, this means
  # calling the doctor.
  idle: ->
    if @idle_handler
      @idle_handler()
    else
      @call_doctor()
  
  # Let the doctor figure out what's wrong here
  call_doctor: ->
    console.log "Oops... calling the doctor!" if @verbose
    @doc ?= new Doctor @deps, @state, @_seed
    @doc.diagnose()
    if @doc.is_stuck()
      @doc.report() if @verbose
      @recover()
    else
      console.log "Hmm, the doctor couldn't find anything amiss..." if @verbose
  
  # Recover from a stuck process.
  recover: ->
    if @doc.recoverable()
      for key, deps of @doc.cycle_dependencies()
        return @fail_recovery() if @cycle_keys[key]
        @signal_worker_of_cycles key, deps
    else
      @fail_recovery()
  
  # Tell the given worker that they have cycle dependencies.
  signal_worker_of_cycles: (key, deps) ->
    @cycle_keys[key] = true
    @remove_dependencies key, deps
    msg = ['cycle', key, deps...].join consts.key_sep
    @db.publish @control_channel, msg
  
  # Remove given dependencies from the key
  remove_dependencies: (key, deps) ->
    @count[key] -= deps.length
    @deps[dep] = _.without @deps[dep], key for dep in deps
  
  # Recovery failed, let the callback know about it.
  fail_recovery: ->
    @stuck_callback?(@doc, @db)
  
  # Signal a job to run again by sending a resume message
  reschedule: (key) ->
    delete @count[key]
    return @unseed() if key == '!seed'
    return if @state[key] == 'done'
    @db.publish @control_channel, "resume#{consts.key_sep}#{key}"
  
  # Handle a request we've never seen before from a given source
  # job that depends on the given keys.
  new_request: (source, keys) ->
    @reqs = []
    @reset_timeout()
    @count[source] = 0
    @handle_request source, keys

  # Handle the requested keys by marking them as dependencies
  # and turning any unsatisfied ones into new jobs.
  handle_request: (source, keys) ->
    for key in _.uniq keys
      @mark_dependency source, key
    if @count[source]
      @request_dependencies()
    else
      @reschedule source

  # Mark the key as a dependency of the given source job. If
  # the key is already completed, then do nothing; if it has
  # not been previously requested, create a new job for it.
  mark_dependency: (source, key) ->
    switch @state[key]
      when 'done' then return
      when undefined then @reqs.push key
    (@deps[key] ?= []).push source
    @unmet++
    @count[source]++
  
  # Take the unmet dependencies from the latest request and push
  # them onto the `jobs` queue.
  request_dependencies: ->
    for req in @reqs
      @state[req] = 'wait'
      @db.rpush 'jobs', req
  
  # Provide a callback to be called when the dispatcher detects the process is stuck
  on_stuck: (callback) ->
    @stuck_callback = callback
    this
        

module.exports =

  # Create a new dispatcher instance and start it listening for
  # requests. Then return the dispatcher.
  run: (options) ->
    dispatcher = new Dispatcher(options ? {})
    dispatcher.listen()
    dispatcher
