redeye_suite = require './support/redeye_suite'

class SomeObj
  constructor: (value: @value) ->
  json: -> value: @value
  do_stuff: -> @value * 2

module.exports = redeye_suite

  'test wrapping with an object':
  
    workers:
      foo: -> @get_now('bar', as: SomeObj).do_stuff()
      bar: -> new SomeObj(value: 7)

    # Kick off by requesting 'foo'
    setup: -> @request 'foo'

    # Assert that the correct dependency graph is generated in
    # the audit log. Note that there are exactly two valid
    # total orderings of the dependency graph produced above.
    expect: ->
      @db.mget 'foo', 'bar', (e, [foo, bar]) =>
        @assert.equal foo, '14'
        @assert.equal bar, '{"value":7}'
        @finish()
