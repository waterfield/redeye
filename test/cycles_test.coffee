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
  @get 'c'
  @for_reals()
  # never get here

worker 'c', ->
  @get 'a'
  setTimeout (=> @emit 'c', 216), 1000
  @for_reals()
  # never gets here

the_dispatcher = null

module.exports = redeye_suite ->

  'idle test':

    setup: (db, dispatcher, finish) ->
      the_dispatcher = dispatcher
      db.publish 'requests', 'a'

    expect: (db, assert, finish) ->
      assert.eql the_dispatcher.doc.cycles[0], ['a', 'b', 'c']
      finish()
