# Test that a simple set of jobs can work together.

# Dependencies.
worker = require 'worker'
debug = require 'debug'
redeye_suite = require './support/redeye_suite'

# Worker: add two other keys together
worker 'add', (a, b) ->
  a = @get a
  b = @get b
  @for_reals()
  a + b

# Worker: produce a random number
worker 'rand', ->
  Math.random()


module.exports = redeye_suite ->

  'simple test':

    setup: (db) ->
      db.publish 'requests', 'add:rand:rand'
      debug.log 'published request'

    expect: (db, assert, finish) ->
      db.get 'add:rand:rand', (err, str) ->
        assert.match str, /[01]\.[0-9]+/
        finish()

