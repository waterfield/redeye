# The redeye test suite can be used in place of a normal expresso test. It
# handles managing multiple Redis databases, starting and stopping
# the Dispatcher and WorkQueue, and timing of setup versus expectations.
# 
# To use it, instead of a single expresso function, you provide a hash
# that looks like this:
# 
#     redeye_suite = require './support/redeye_suite'
#     
#     module.exports = redeye_suite
#       'name of test':
#         
#         workers:
#           worker_name: (args...) ->
#             # Here you define a normal redeye worker. This is the
#             # same as calling `worker worker_name, (args...) ->`
#         
#         setup:
#           # Use @db to access Redis
#           # Use @request(key) to kick off the redeye tasks
#           # Access @dispatcher to add custom event handling
#         
#         expect:
#           # Use @db and @assert to test your workers
#           # Be sure to call @finish() when you're done
# 
# You can see examples in `redeye/test/*_test.coffee`.

# Dependencies.
dispatcher = require '../../lib/dispatcher'
redeye = require '../../lib/redeye'
consts = require '../../lib/consts'
AuditListener = require './audit_listener'
db = require '../../lib/db'
_ = require 'underscore'
require '../../lib/util'

db_index = 4

# Test class for replacing a single expresso test.
class RedeyeTest
  
  # Process the given expresso test. The `@run` method of the resulting
  # instance can be used to execute the altered test.
  constructor: (test, @exit, @assert) ->
    {setup: @setup, expect: @expect, workers: @workers} = test
    @db_index = ++db_index
    @db = db @db_index
    @audit = new AuditListener
    @opts = test_mode: true, db_index: @db_index, audit: @audit
    @queue = redeye.queue @opts
    @add_workers()
  
  # Add the workers defined by the `workers` key of the test to
  # the WorkQueue we control.
  add_workers: ->
    for name, fun of @workers ? {}
      @queue.worker name, fun

  # Run the test. This differs from a normal expresso test in the following ways:
  # 
  #  - Chooses a unique Redis db index and channel namespace
  #  - Flushes the database at the start of the test
  #  - Starts a Dispatcher and a WorkQueue
  #  - Uses the `setup` key to initialize the test
  #  - Waits until the WorkQueue terminates, then calls `expect`
  #  - Has an emergency timeout that kills the redeye processes
  #  - Waits on `@finish` to be called to complete the test
  run: ->
    @db.flushdb =>
      @dispatcher = dispatcher.run @opts
      @queue.run => @expect.apply this
      setTimeout (=> @setup.apply this), 100
      @timeout = setTimeout (=> @die()), 5000

  # Forcefully quit the test
  die: ->
    @dispatcher.quit()
    @finish()

  # Terminate the last redis connection, ending the test
  finish: ->
    clearTimeout @timeout
    @db.end()
  
  # Send a request to the correct `requests` channel
  request: (args...) ->
    @requested = args.join consts.arg_sep
    @db.publish _('requests').namespace(@db_index), @requested
  
  # Set a redis value, but first convert to JSON
  set: (args..., value) ->
    key = args.join consts.arg_sep
    @db.set key, JSON.stringify(value)

  # Look up and de-jsonify a value from redis
  get: (args..., callback) ->
    key = args.join consts.arg_sep
    @db.get key, (err, str) ->
      throw err if err
      callback JSON.parse(str)

# This file exports a method which replaces
# a whole set of tests. See the comment at the
# top of this file or look in `redeye/test/*_test.coffee`
# for examples.
module.exports = (tests) ->
  for name, test of tests
    do (name, test) ->
      tests[name] = (exit, assert) ->
        new RedeyeTest(test, exit, assert).run()
  tests
