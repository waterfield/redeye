redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  'test @all':
  
    workers:
      multi: ->
        @all ->
          @get 'a'
          @get 'b'
      a: -> 3
      b: -> 5

    # Request that two random numbers be added together. Actually, only one
    # random number will be created; the key 'rand' is bound to a random number,
    # and then that same number is used again.
    setup: ->
      @request 'multi'

    # Make sure that adding two random numbers results in
    # a number between zero and two.
    expect: ->
      @get @requested, (val) =>
        @assert.eql val, [3,5]
        @finish()
