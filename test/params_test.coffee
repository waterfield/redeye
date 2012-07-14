redeye_suite = require './support/redeye_suite'
Workspace = require '../lib/workspace'

class Subspace extends Workspace
  constructor: (@b) ->
  value: -> @x()

module.exports = redeye_suite 

  'test named params come through as @ locals':        
    setup: ->
      @queue.worker 'x', 'a', 'b', -> @a + @b
      @request 'x', 'foo', 'bar'
    expect: ->
      @get @requested, (value) =>
        @assert.equal value, 'foobar'
        @finish()
  
  'test we can request named worker via object':
    setup: ->
      @queue.worker 'x', 'a', 'b', -> @a + @b
      @queue.worker 'y', -> @x b: 'bar', a: 'foo'
      @request 'y'
    expect: ->
      @get @requested, (value) =>
        @assert.equal value, 'foobar'
        @finish()

  'test that locals are re-used for @get':
    setup: ->
      @queue.worker 'x', 'a', 'b', -> @a + @b
      @queue.worker 'y', 'a', 'b', -> @x()
      @request 'y', 'foo', 'bar'
    expect: ->
      @get @requested, (value) =>
        @assert.equal value, 'foobar'
        @finish()
  
  'test that sub-workspaces can override some locals':
    setup: ->
      @queue.worker 'x', 'a', 'b', -> @a + @b
      @queue.worker 'y', 'a', 'b', -> new Subspace('baz').value()
      @request 'y', 'foo', 'bar'
    expect: ->
      @get @requested, (value) =>
        @assert.equal value, 'foobaz'
        @finish()
