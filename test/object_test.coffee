redeye_suite = require './support/redeye_suite'

class SomeObj
  constructor: (value: @value) ->
  do_stuff: -> @value * 2
  baz: -> @bless(new OtherObj).do_stuff()

class OtherObj
  do_stuff: -> @get_now 'quux'

module.exports = redeye_suite

  'test wrapping with an object':
  
    workers:
      foo: -> @get_now('bar', as: SomeObj).do_stuff()
      bar: -> new SomeObj(value: 7)
      baz: -> @get_now('bar', as: SomeObj).baz()
      quux: -> 216
      all: (keys...) -> @get key for key in keys; @for_reals()

    # Kick off by requesting 'foo'
    setup: -> @request 'all', 'foo', 'baz'

    # Assert that the correct dependency graph is generated in
    # the audit log. Note that there are exactly two valid
    # total orderings of the dependency graph produced above.
    expect: ->
      @db.mget 'foo', 'bar', 'baz', 'quux', (e, [foo, bar, baz, quux]) =>
        @assert.equal foo, '14'
        @assert.equal bar, '{"value":7}'
        @assert.equal baz, '216'
        @assert.equal quux, '216'
        @finish()

    