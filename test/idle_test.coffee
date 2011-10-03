# Test that the idle handler is called.

# Dependencies.
worker = require 'worker'
debug = require 'debug'
redeye_suite = require './support/redeye_suite'

worker 'a', ->
  @get 'b'
  @for_reals()
  # never get here

worker 'b', ->
  @emit 'c', 216
  setTimeout (=> @emit 'b', 42), 1000

idled = false

module.exports = redeye_suite ->

  'idle test':

    setup: (db, dispatcher, finish) ->
      dispatcher.when_idle -> idled = true
      db.publish 'requests', 'a'

    expect: (db, assert, finish) ->
      assert.equal idled, true
      finish()
