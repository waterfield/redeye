dispatcher = require 'dispatcher'
redeye = require 'redeye'
debug = require 'debug'
AuditListener = require './audit_listener'
db = require 'db'

db_index = 0

class RedeyeTest
  
  constructor: (test, @exit, @assert) ->
    {setup: @setup, expect: @expect, workers: @workers} = test
    @db_index = ++db_index
    @db = db @db_index
    @audit = new AuditListener
    @opts = test_mode: true, db_index: @db_index, audit: @audit
    @queue = redeye.queue @opts
    @add_workers()
  
  add_workers: ->
    for name, fun of @workers
      debug.log "test: adding worker: #{name}"
      @queue.worker name, fun

  run: ->
    @db.flushdb =>
      @dispatcher = dispatcher.run @opts
      @queue.run => @expect.apply this
      setTimeout (=> @setup.apply this), 100
      @timeout = setTimeout (=> @finish()), 5000

  finish: ->
    clearTimeout @timeout
    @db.end()
  
  request: (key) ->
    @db.publish "requests_#{@db_index}", key


module.exports = (tests) ->
  for name, test of tests
    tests[name] = (exit, assert) ->
      new RedeyeTest(test, exit, assert).run()
  tests
