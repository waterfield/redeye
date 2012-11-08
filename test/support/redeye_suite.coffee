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
    @_kv = db.key_value {@db_index}
    @_pubsub = db.pub_sub {@db_index}
    @audit = new AuditListener
    @opts = test_mode: true, db_index: @db_index, audit: @audit
    @queue = redeye.queue @opts
    @add_workers()

  connect: (callback) ->
    @_kv.connect =>
      @_pubsub.connect callback

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
    @connect =>
      @_kv.flush =>
        @dispatcher = dispatcher.run @opts
        @dispatcher.connect =>
          @queue.run => @expect.apply this
          setTimeout (=> (@fiber = Fiber => @setup.apply this).run()), 100
          @timeout = setTimeout (=> @die()), 5000

  # Forcefully quit the test
  die: ->
    @dispatcher.quit()
    @finish()
    @assert.ok false, "Timed out, sad panda"

  diff_message: (expected, actual, prefix='') ->
    if _.isArray expected
      @array_diff_message expected, actual, prefix
    else if _.isObject expected
      @object_diff_message expected, actual, prefix
    else
      @value_diff_message expected, actual, prefix

  value_diff_message: (expected, actual, prefix='') ->
    "#{prefix}- #{expected}\n#{prefix}+ #{actual}\n"

  array_diff_message: (expected, actual, prefix='') ->
    len = Math.max(expected.length, actual.length)
    msg = ''
    for i in [0...len]
      a = expected[i]
      b = actual[i]
      if a and b
        unless _.isEqual(a,b)
          msg += "[#{i}]\n" + @diff_message(a,b,prefix+'  ')
      else if a
        msg += "[#{i}] - #{JSON.stringify(a)}\n"
      else if b
        msg += "[#{i}] + #{JSON.stringify(b)}\n"
    msg

  object_diff_message: (expected, actual, prefix='') ->
    msg = ''
    for k, a of expected
      if (b = actual[k])?
        if !_.isEqual(a, b)
          msg += "#{prefix}[#{k}]\n#{@diff_message a, b, prefix+'  '}"
      else
        msg += "#{prefix}[#{k}]\n#{prefix}  - #{JSON.stringify(a)}\n"
    for k, b of actual
      unless k of expected
        msg += "#{prefix}[#{k}]\n#{prefix}  + #{JSON.stringify(b)}\n"
    msg

  error_message: (key, parts) ->
    list = ["Key \"#{key}\" encountered an error\n"]
    for item in parts
      { key, slice, trace } = item
      list.push "In worker: [#{slice}] #{key}"
      for line in trace.split "\n"
        list.push "    #{line}"
    list.join("\n")

  delete_nodes: (val) ->
    if _.isArray val
      for item, i in val
        delete item.node_id if item
    else if _.isObject val
      delete val.node_id
    val

  want: (key_parts..., expected) ->
    key = if key_parts.length
      key_parts.join consts.arg_sep
    else
      @requested
    @get key, (actual) =>
      @finish()
      if actual.error
        msg = @error_message key, actual.error
        @assert.ok false, msg
      else
        actual = @delete_nodes actual
        if _.isEqual actual, expected
          @assert.ok true
        else
          msg = "Key \"#{key}\" was wrong:\n\n"
          msg = msg + @diff_message(expected, actual)
          @assert.ok false, msg

  # Terminate the last redis connection, ending the test
  finish: ->
    clearTimeout @timeout
    @_kv.end()
    @_pubsub.end()
    delete @fiber

  # Send a request to the correct `requests` channel
  request: (args...) ->
    @requested = args.join consts.arg_sep
    @_pubsub.publish _('requests').namespace(@db_index), @requested

  set: (args..., value) ->
    key = args.join consts.arg_sep
    @_kv.del key, =>
      @_kv.set key, value, =>
        @fiber.run()
    yield()

  # Look up and de-jsonify a value from redis
  get: (args..., callback) ->
    key = args.join consts.arg_sep
    @_kv.get key, (err, val) ->
      throw err if err
      callback val

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
