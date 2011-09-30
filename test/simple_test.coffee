# Test that a simple set of jobs can work together.

# Dependencies.
db = require('db')()
require './workers/add'
require './workers/rand'

assert = null

run_tests = ->
  exports['simple test'] = (exit, the_assert) ->
    assert = the_assert
    console.log "assert =", the_assert, ", exit:", exit
    db.publish 'requests', 'add:rand:rand'
    console.log "test: publish request: add:rand:rand"

expectations = (callback) ->
  db.get 'add:rand:rand', (err, str) ->
    assert.isNull err
    assert.isNotNull str
    assert.match str, /[01]\.[0-9]+/
    callback?()

cleanup = ->
  db.end()

start = ->
  db.flushall ->

    # Start the dispatcher and a worker.
    require('../lib/dispatcher').run(true)
    require('../lib/redeye').run -> expectations cleanup
  
    setTimeout run_tests, 100

start()