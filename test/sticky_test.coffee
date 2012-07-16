redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  # Tests that the sticky cache is working.
  'test the sticky cache':
  
    workers:
      # Make sure requesting something already stickied will
      # use that value and not request it from the dispatcher.
      a: -> @worker().sticky.z = 216; @get 'z'
      # Make sure a stickied request gets stored in the cache.
      b: -> @get 'c', sticky: true; @worker().sticky.c
      c: -> 42
      _all: -> @get 'a'; @get 'b'

    # Kick off by requesting 'a'
    setup: ->
      @request '_all'

    # Assert that the sticky tests are set to the right values.
    expect: ->
      @_kv.get_all ['a', 'b'], (err, [a,b]) =>
        @assert.equal a, 216
        @assert.equal b, 42
        @finish()
