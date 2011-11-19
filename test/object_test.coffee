redeye_suite = require './support/redeye_suite'

# This class is instantiated and `@bless`'d auto-magically by Redeye.
class SomeObj
  constructor: (value: @value) ->
  do_stuff: -> @value * 2
  baz: -> @bless(new OtherObj).do_stuff()

# This class' object should be recursovely blessed,
# giving it access to the `@get_now` method.
class OtherObj
  do_stuff: -> @quux()

module.exports = redeye_suite

  'test wrapping with an object':
  
    workers:
      # `foo` grabs `bar` and calls a method on it
      foo: -> @get_now('bar', as: SomeObj).do_stuff()
      # `bar` produces an object that can be reconstructed with `as:`
      bar: -> new SomeObj(value: 7)
      # `baz` grabs `bar` then calls a method that requires blessings
      baz: -> @get_now('bar', as: SomeObj).baz()
      # `quux` is required by the `baz` method of `bar` (um... names are hard?)
      quux: -> 216
      # go grab all the requested keys
      all: (keys...) -> @get key for key in keys; @for_reals()

    # Kick off by requesting 'foo' and 'baz'
    setup: ->
        @queue.mixin
          quux: -> @get_now 'quux'
        @request 'all', 'foo', 'baz'

    # Assert that all the keys get set correctly. `bar` should be a JSON blob.
    # `foo` should be twice it's `value` key. `quux` should be just 216, and
    # `baz` should be set to `quux`.
    expect: ->
      @db.mget 'foo', 'bar', 'baz', 'quux', (e, [foo, bar, baz, quux]) =>
        @assert.equal foo, '14'
        @assert.equal bar, '{"value":7}'
        @assert.equal baz, '216'
        @assert.equal quux, '216'
        @finish()

    