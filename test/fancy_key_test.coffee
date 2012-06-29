redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite 

  'fancy test of key accessors':
  
    workers:
      foo: (x) -> x * 2
      test: -> @foo 3
        
    # Set the 'x:*' keys, then request a sum
    setup: ->
      @request 'test'

    # Assert that the doctor ran, and that it detected
    # our cyclic dependency.
    expect: ->
      @get @requested, (value) =>
        @assert.equal value, 6
        @finish()
