redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  # Tests that the sticky cache is working.
  'test the sticky cache':
  
    workers:
      # Make sure requesting something already stickied will
      # use that value and not request it from the dispatcher.
      a: -> @sticky.z = 216; @get 'z'
      # Make sure a stickied request gets stored in the cache.
      b: -> @get_now 'c', sticky: true; @sticky.c
      c: -> 42
      all: -> @get 'a'; @get 'b'; @for_reals()

    # Kick off by requesting 'a'
    setup: ->
      @request 'all'

    # Assert that the sticky tests are set to the right values.
    expect: ->
      @db.mget 'a', 'b', (err, [a,b]) =>
        @assert.equal a, 216
        @assert.equal b, 42
        @finish()
