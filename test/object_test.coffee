redeye_suite = require './support/redeye_suite'
Workspace = require '../lib/workspace'

# This class is instantiated and `@bless`'d auto-magically by Redeye.
class SomeObj
  constructor: (value: @value) ->
  do_stuff: -> @value * 2
  baz: -> (new OtherObj).do_stuff()

class OtherObj extends Workspace
  do_stuff: -> @quux()

module.exports = redeye_suite

  'test wrapping with an object':
  
    workers:
      # `foo` grabs `bar` and calls a method on it
      foo: -> @get('bar', as: SomeObj).do_stuff()
      # `bar` produces an object that can be reconstructed with `as:`
      bar: -> new SomeObj(value: 7)
      # `baz` grabs `bar` then calls a method that requires blessings
      baz: -> @get('bar', as: SomeObj).baz()
      # `quux` is required by the `baz` method of `bar` (um... names are hard?)
      quux: -> 216
      # go grab all the requested keys
      all: (keys...) -> @get key for key in keys

    # Kick off by requesting 'foo' and 'baz'
    setup: ->
        @queue.mixin
          quux: -> @get 'quux'
        @request 'all', 'foo', 'baz'

    # Assert that all the keys get set correctly. `bar` should be a JSON blob.
    # `foo` should be twice it's `value` key. `quux` should be just 216, and
    # `baz` should be set to `quux`.
    expect: ->
      @_kv.get_all ['foo', 'bar', 'baz', 'quux'], (e, [foo, bar, baz, quux]) =>
        @assert.eql foo, 14
        @assert.eql bar, {value: 7}
        @assert.eql baz, 216
        @assert.eql quux, 216
        @finish()

    