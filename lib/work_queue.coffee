Worker = require './worker'
events = require 'events'
consts = require './consts'
db = require './db'
_ = require 'underscore'
require './util'

# The `WorkQueue` accepts job requests and starts `Worker` objects
# to handle them.
class WorkQueue extends events.EventEmitter

  # Register the 'next' event, and listen for 'resume' messages.
  constructor: (@options) ->
    @db = db @options.db_index
    @resume = db @options.db_index
    @control = db @options.db_index
    @worker_db = db @options.db_index
    @workers = {}
    @runners = {}
    @sticky = {}
    @mixins = {}
    @listen()
    @on 'next', => @next()
  
  # Subscribe to channels
  listen: ->
    @resume.on 'message', (channel, key) =>
      @workers[key]?.resume()
    @resume.subscribe _('resume').namespace(@options.db_index)

    @control.on 'message', (channel, msg) => @perform msg
    @control.subscribe _('control').namespace(@options.db_index)
  
  # React to a control message sent by the dispatcher
  perform: (msg) ->
    [action, args...] = msg.split consts.key_sep
    switch action
      when 'quit' then @quit()
      when 'reset' then @reset()
      when 'cycle' then @cycle_detected args...
  
  # The dispatcher is telling us the given key is part of a cycle. If it's one
  # of ours, cause the worker to re-run, but throwing an error from the @get that
  # caused the cycle. On the plus side, we can assume that all the worker's non-
  # cycled dependencies have been met now.
  cycle_detected: (key, dependencies...) ->
    if worker = @workers[key]
      for dep in dependencies
        worker.cycle[dependency] = true
  
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
    @control.end()
    @worker_db.end()
    @callback?()
  
  # Clean out the sticky cache
  reset: ->
    console.log 'worker resetting!' # XXX
    @sticky = {}
    
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
