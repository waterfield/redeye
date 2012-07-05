Workspace = require './workspace'
Worker = require './worker'
events = require 'events'
consts = require './consts'
db = require './db'
_ = require 'underscore'
require './util'
util = require 'util'

# The `WorkQueue` accepts job requests and starts `Worker` objects
# to handle them.
class WorkQueue extends events.EventEmitter

  # Register the 'next' event, and listen for 'resume' messages.
  constructor: (@options) ->
    @_kv = db.key_value @options
    @_queue = db.queue @options
    @_pubsub = db.pub_sub @options
    @_worker_kv = db.key_value @options
    @_worker_pubsub = db.pub_sub @options
    @workers = {}
    @runners = {}
    @sticky = {}
    @mixins = {}
    @_worker_count = 0
    @listen()
    @on 'next', => @next()
  
  # Subscribe to channels
  listen: ->
    @_pubsub.message (channel, msg) => @perform msg
    @_pubsub.subscribe _('control').namespace(@options.db_index)
  
  # React to a control message sent by the dispatcher
  perform: (msg) ->
    [action, args...] = msg.split consts.key_sep
    switch action
      when 'resume' then @resume args...
      when 'erase' then @erase args...
      when 'quit' then @quit()
      when 'reset' then @reset()
      when 'cycle' then @cycle_detected args...
      when 'info' then @dump_info()
  
  dump_info: ->
    console.log util.inspect(this, false, null, true)
      
  # Resume the given worker (if it's one of ours)
  resume: (key) ->
    @workers[key]?.resume()
  
  # Erase the given key from the sticky cache (it was invalidated).
  erase: (key) ->
    delete @sticky[key]

  # The dispatcher is telling us the given key is part of a cycle. If it's one
  # of ours, cause the worker to re-run, but throwing an error from the @get that
  # caused the cycle. On the plus side, we can assume that all the worker's non-
  # cycled dependencies have been met now.
  cycle_detected: (key, dependencies...) ->
    @workers[key]?.cycle()# dependencies
  
  # Run the work queue, calling the given callback on completion
  run: (@callback) ->
    @next()  
    
  # Add a worker to the context
  worker: (prefix, runner) ->
    @runners[prefix] = runner
    shortcut = {}
    shortcut[prefix] = (args...) -> @get prefix, args...
    Workspace.mixin shortcut

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
        @workers[str] = new Worker(str, this, @sticky)
        @workers[str].run()
        # console.log @_worker_count
      catch e
        @error e unless e == 'no_runner'
      @emit 'next'
  
  # Shut down the redis connection and stop running workers
  quit: ->
    @_kv.end()
    @_queue.end()
    @_pubsub.end()
    @_worker_kv.end()
    @_worker_pubsub.end()
    @callback?()
  
  # Clean out the sticky cache
  reset: ->
    console.log 'worker resetting'
    @sticky = {}
    
  # Mark the given worker as finished (release its memory)
  finish: (key) ->
    @_worker_count--
    delete @workers[key]
  
  # Mark that a fatal exception occurred
  error: (err) ->
    message = err.stack ? err
    console.log message
    @_kv.set 'fatal', message
  
  # Print a debugging statement
  debug: (args...) ->
    #console.log 'queue:', args...
  
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
