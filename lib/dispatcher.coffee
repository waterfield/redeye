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
    @resume_channel = _('resume').namespace @options.db_index
    @count = {}
    @state = {}
    @deps = {}
    @unmet = 0

  # Subscribe to the `requests` and `responses` channels.
  listen: ->
    @req.on 'message', (ch, str) => @requested str
    @res.on 'message', (ch, str) => @responded str
    @req.subscribe _('requests').namespace(@options.db_index)
    @res.subscribe _('responses').namespace(@options.db_index)
    setInterval (=> @status()), 1000
  
  # Print a status report.
  status: ->
    @tick('' + @unmet)

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
  
  # Forget everything we know about dependency state.
  reset: ->
    @count = {}
    @state = {}
    @deps = {}
  
  # Called when a key is completed. Any jobs depending on this
  # key are updated, and if they have no more dependencies, are
  # signalled to run again.
  responded: (key) ->
    @audit "!#{key}"
    @state[key] = 'done'
    targets = @deps[key] ? []
    delete @deps[key]
    @tick '!'
    @progress targets

  # Write text to the audit stream
  audit: (text) ->
    @audit_stream.write "#{text}\n" if @audit_stream

  # The given key is a 'seed' request. In test mode, completion of
  # the seed request signals termination of the workers.
  seed: (key) ->
    @_seed = key
    @tick 'S'
    @new_request '!seed', [key]
  
  # The seed request was completed. In test mode, quit the workers.
  unseed: ->
    @clear_timeout()
    @quit() if @test_mode
  
  # Send quit signals to the work queues.
  quit: ->
    @clear_timeout()
    for i in [1..100]
      @db.rpush 'jobs', '!quit'
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
  when_idle: (@idle_handler) ->
  
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
    @doc.report()
  
  # Signal a job to run again by sending a resume message
  reschedule: (key) ->
    delete @count[key]
    return @unseed() if key == '!seed'
    @tick '/'
    @db.publish @resume_channel, key
  
  # Handle a request we've never seen before from a given source
  # job that depends on the given keys.
  new_request: (source, keys) ->
    @reqs = []
    @reset_timeout()
    @count[source] = 0
    @tick '.'
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
      @tick '+'
      @state[req] = 'wait'
      @db.rpush 'jobs', req
  
  # Make a little note
  tick: (sym) ->
    process.stdout.write(sym) if @verbose
      

module.exports =

  # Create a new dispatcher instance and start it listening for
  # requests. Then return the dispatcher.
  run: (options) ->
    dispatcher = new Dispatcher(options ? {})
    dispatcher.listen()
    dispatcher
