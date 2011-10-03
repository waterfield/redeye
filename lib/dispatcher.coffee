# Dependencies.
consts = require './consts'
debug = require './debug'
req = require('./db')()
res = require('./db')()
db = require('./db')()

audit_stream = null

# The dispatcher accepts requests for keys and manages the
# dependencies between jobs. It ensures that the same work
# is never requested more than once, and makes sure jobs are
# re-run whenever their dependencies are met.
class Dispatcher

  # Initializer
  constructor: (@test_mode=false) ->
    @count = {}
    @state = {}
    @deps = {}
    
  # Subscribe to the `requests` and `responses` channels.
  listen: ->
    req.on 'message', (ch, str) => @requested str
    res.on 'message', (ch, str) => @responded str
    req.subscribe 'requests'
    res.subscribe 'responses'

  # Called when a worker requests keys. The keys requested are
  # recorded as dependencies, and any new key requests are
  # turned into new jobs.
  requested: (str) ->
    debug.log "dispatcher: requested: #{str}"
    [source, keys...] = str.split consts.key_sep
#    return if @state[source]
    if keys.length
      @audit "?#{str}"
      @new_request source, keys
    else
      @seed source
  
  # Called when a key is completed. Any jobs depending on this
  # key are updated, and if they have no more dependencies, are
  # signalled to run again.
  responded: (key) ->
    debug.log "dispatcher: responded: #{key}"
    @audit "!#{key}"
    @state[key] = 'done'
    targets = @deps[key] ? []
    delete @deps[key]
    @progress targets

  # Write text to the audit stream
  audit: (text) ->
    audit_stream.write "#{text}\n" if audit_stream

  # The given key is a 'seed' request. In test mode, completion of
  # the seed request signals termination of the workers.
  seed: (key) ->
    debug.log "dispatcher: seed: #{key}"
    @new_request '!seed', [key]
  
  # The seed request was completed. In test mode, quit the workers.
  unseed: ->
    debug.log "dispatcher: unseed"
    @quit() if @test_mode
  
  # Send quit signals to the work queues.
  quit: ->
    for i in [1..100]
      db.rpush 'jobs', '!quit'
    finish = ->
      db.del 'jobs'
      req.end()
      res.end()
      db.end()
    setTimeout finish, 500

  # Make progress on each of the given keys by decrementing
  # their count of remaining dependencies. When any reaches
  # zero, it is rescheduled.
  progress: (keys) ->
    for key in keys
      unless --@count[key]
        @reschedule key
  
  # Signal a job to run again by sending a resume message
  reschedule: (key) ->
    delete @count[key]
    return @unseed() if key == '!seed'
    db.publish 'resume', key
  
  # Handle a request we've never seen before from a given source
  # job that depends on the given keys.
  new_request: (source, keys) ->
    @reqs = []
    @count[source] = 0
    debug.log "dispatcher: new_request: source:", source, "keys:", keys
    @handle_request source, keys

  # Handle the requested keys by marking them as dependencies
  # and turning any unsatisfied ones into new jobs.
  handle_request: (source, keys) ->
    for key in @unique(keys)
      @mark_dependency source, key
    if @count[source]
      @request_dependencies()
    else
      debug.log "dispatcher: already satisfied: #{source}"
      @reschedule source

  # Mark the key as a dependency of the given source job. If
  # the key is already completed, then do nothing; if it has
  # not been previously requested, create a new job for it.
  mark_dependency: (source, key) ->
    switch @state[key]
      when 'done' then return
      when undefined then @reqs.push key
    debug.log "dispatcher: #{source} now depends on #{key}"
    (@deps[key] ?= []).push source
    @count[source]++

  # Find unique elements of a list (kinda the wrong place for this...)
  unique: (list) ->
    hash = {}
    uniq = []
    for elem in list
      uniq.push elem unless hash[elem]
      hash[elem] = true
    uniq

  # Take the unmet dependencies from the latest request and push
  # them onto the `jobs` queue.
  request_dependencies: ->
    for req in @reqs
      debug.log "dispatcher: asking for: #{req}"
      @state[req] = 'wait'
      db.rpush 'jobs', req
      

module.exports =

  run: (test_mode=false) ->
    new Dispatcher(test_mode).listen()
  
  audit: (stream) ->
    audit_stream = stream ? audit_stream
