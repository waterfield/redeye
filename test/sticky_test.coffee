redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  # Tests the audit trail produced by the dispatcher
  'test the sticky cache':
  
    workers:
      a: -> @sticky.z = 216; @get 'z'
      b: -> @get_now 'c', sticky: true; @sticky.c
      c: -> 42
      all: -> @get 'a'; @get 'b'; @for_reals()

    # Kick off by requesting 'a'
    setup: ->
      @request 'all'

    # Assert that the correct dependency graph is generated in
    # the audit log. Note that there are exactly two valid
    # total orderings of the dependency graph produced above.
    expect: ->
      @db.mget 'a', 'b', (err, [a,b]) =>
        @assert.equal a, 216
        @assert.equal b, 42
        @finish()
