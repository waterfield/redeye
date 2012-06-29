redeye_suite = require './support/redeye_suite'

req1 = []
req2 = []

module.exports = redeye_suite

  'test @all':
  
    workers:
      multi: ->
        @all ->
          @get 'c'
          @get 'b'
          @get 'a'
      a: ->
        req1.push 'a'
        @emit 'q', 0
        setTimeout (=> req2.push 'a'; @emit 'a', 1), 100
      b: ->
        req1.push 'b'
        @emit 'q', 0
        setTimeout (=> req2.push 'b'; @emit 'b', 2), 200
      c: ->
        req1.push 'c'
        @emit 'q', 0
        setTimeout (=> req2.push 'c'; @emit 'c', 3), 300

    # Request that two random numbers be added together. Actually, only one
    # random number will be created; the key 'rand' is bound to a random number,
    # and then that same number is used again.
    setup: ->
      @request 'multi'

    # Make sure that adding two random numbers results in
    # a number between zero and two.
    expect: ->
      @get @requested, (val) =>
        @assert.eql val, [3,2,1]
        @assert.eql req1, ['c','b','a']
        @assert.eql req2, ['a','b','c']
        @finish()
