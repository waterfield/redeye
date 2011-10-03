# Test that jobs can accept arguments

# Dependencies.
worker = require 'worker'
debug = require 'debug'
redeye_suite = require './support/redeye_suite'

worker 'problem', ->
  a = @get 'x', 2, 3
  b = @get 'x', 1, 7
  @for_reals()
  a + b

worker 'x', (a, b) ->
  parseInt(a) * parseInt(b)


module.exports = redeye_suite ->

  'test of multi args':

    setup: (db) ->
      db.publish 'requests', 'problem'

    expect: (db, assert, finish) ->
      db.get 'problem', (err, str) ->
        assert.equal str, '13'
        finish()
