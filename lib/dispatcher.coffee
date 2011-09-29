# Dependencies.
consts = require('./consts')
req = require('./db')()
res = requre('./db')()
db = require('./db')()

# The dispatcher accepts requests for keys and manages the
# dependencies between jobs. It ensures that the same work
# is never requested more than once, and makes sure jobs are
# re-run whenever their dependencies are met.
class Dispatcher

  # Subscribe to the `requests` and `responses` channels.
  initialize: ->
    req.on 'message', (ch, str) => @requested str
    res.on 'message', (ch, str) => @responded str
    req.subscribe 'requests'
    res.subscribe 'responses'
    @count = {}
    @state = {}
    @deps = {}

  # Called when a worker requests keys. The keys requested are
  # recorded as dependencies, and any new key requests are
  # turned into new jobs.
  requested: (str) ->
    [source, keys...] = str.split consts.key_sep
    return if @state[source]
    @new_request source, keys

  # Called when a key is completed. Any jobs depending on this
  # key are updated, and if they have no more dependencies, are
  # signalled to run again.
  responded: (key) ->
    @state[key] = 'done'
    targets = @deps[key] ? []
    delete @deps[key]
    @progress targets
  
  # Make progress on each of the given keys by decrementing
  # their count of remaining dependencies. When any reaches
  # zero, it is rescheduled.
  progress: (keys) ->
    for key in keys
      unless --@count[key]
        delete @count[key]
        @reschedule key
  
  # Signal a job to run again by pushing onto its blocking lock.
  reschedule: (key) ->
    lock = "resume_#{key}"
    db.rpush lock, 'ok'

  # Handle a request we've never seen before from a given source
  # job that depends on the given keys.
  new_request: (source, keys) ->
    @reqs = []
    @state[source] = 'wait'
    @count[source] = 0
    @handle_request source, keys

  # Handle the requested keys by marking them as dependencies
  # and turning any unsatisfied ones into new jobs.
  handle_request: (source, keys) ->
    for key in keys
      @mark_dependency source, key
    @request_dependencies()

  # Mark the key as a dependency of the given source job. If
  # the key is already completed, then do nothing; if it has
  # not been previously requested, create a new job for it.
  mark_dependency: (source, key) ->
    switch @state[key]
      when 'done' then return
      when undefined then @reqs.push key
    (@deps[key] ?= []).push source
    @count[source]++

  # Take the unmet dependencies from the latest request and push
  # them onto the `jobs` queue.
  request_dependencies: ->
    for req in @reqs
      db.rpush 'jobs', req
