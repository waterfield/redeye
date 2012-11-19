redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  # # Test that a simple set of jobs can work together.
  # 'simple test':

  #   workers:
  #     # The 'add' worker is very simple; its arguments are other keys,
  #     # it looks them up and then adds them.
  #     'add': (a, b) ->
  #       a = @get a
  #       b = @get b
  #       a + b

  #     # 'rand' just resolves to a random number
  #     'rand': -> Math.random()

  #   # Request that two random numbers be added together. Actually, only one
  #   # random number will be created; the key 'rand' is bound to a random number,
  #   # and then that same number is used again.
  #   setup: ->
  #     @request 'add:rand:rand'

  #   # Make sure that adding two random numbers results in
  #   # a number between zero and two.
  #   expect: ->
  #     @_kv.get 'add:rand:rand', (err, str) =>
  #       @assert.match str, /[01]\.[0-9]+/
  #       @finish()


  # 'multiple resolution stages':

  #   workers:
  #     # Requests 'b' multiple times with different arguments
  #     a: ->
  #       b1 = @get 'b', 1
  #       b2 = @get 'b', 2
  #       b3 = @get 'b', 3
  #       b1 + b2 + b3 # heh, these are strings :)
  #     b: (n) -> n

  #   setup: -> @request 'a'

  #   # Test that all the jobs ran correctly
  #   expect: ->
  #     @_kv.get 'a', (err, str) =>
  #       @assert.equal str, '123'
  #       @assert.eql @audit.messages, ['?a|b:1', '!b:1', '?a|b:2', '!b:2', '?a|b:3', '!b:3', '!a']
  #       @finish()


  # It wouldn't be complete without a Fibonacci test!
  'fibonacci':

    workers:
      fib: (n) ->
        n = parseInt n
        return 1 if n < 2
        a = @get 'fib', n-2
        b = @get 'fib', n-1
        a + b

    setup: ->
      @request 'fib', 8

    # Make sure it gets the right answer
    expect: ->
      @want 34
