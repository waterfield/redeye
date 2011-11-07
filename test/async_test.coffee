redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite 

  # Test that async mode works correctly
  'asynchronous test':
  
    workers:
      a: ->
        @db.set 'foo', '5'
        @async ->
          @db.get 'foo', (e, foo) =>
            bar = @get 'bar'
            return unless @for_reals()
            @finish parseInt(foo) * bar

    # Set the 'x:*' keys, then request a sum
    setup: ->
      @set 'bar', '7'
      @request 'a'

    # Assert that the doctor ran, and that it detected
    # our cyclic dependency.
    expect: ->
      @get 'a', (a) =>
        @assert.equal a, 35
        @finish()
