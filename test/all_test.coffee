redeye_suite = require './support/redeye_suite'

req1 = []
req2 = []

module.exports = redeye_suite

  # this also implicitly tests that the job queue is really a stack
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
        worker = @worker()
        setTimeout (=> req2.push 'a'; worker.emit 'a', 1), 100
      b: ->
        req1.push 'b'
        @emit 'q', 0
        worker = @worker()
        setTimeout (=> req2.push 'b'; worker.emit 'b', 2), 200
      c: ->
        req1.push 'c'
        @emit 'q', 0
        worker = @worker()
        setTimeout (=> req2.push 'c'; worker.emit 'c', 3), 300

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
        @assert.eql req2, ['a','b','c']
        @finish()
