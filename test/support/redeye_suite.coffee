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
dispatcher = require 'dispatcher'
redeye = require 'redeye'
debug = require 'debug'
AuditListener = require './audit_listener'
db = require 'db'

db_index = 0

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
      debug.log "test: adding worker: #{name}"
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
  request: (key) ->
    @db.publish "requests_#{@db_index}", key


# This file exports a method which replaces
# a whole set of tests. See the comment at the
# top of this file or look in `redeye/test/*_test.coffee`
# for examples.
module.exports = (tests) ->
  for name, test of tests
    tests[name] = (exit, assert) ->
      new RedeyeTest(test, exit, assert).run()
  tests
