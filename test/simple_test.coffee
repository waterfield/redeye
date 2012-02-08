redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  # Test that a simple set of jobs can work together.
  'simple test':
  
    workers:
      # The 'add' worker is very simple; its arguments are other keys,
      # it looks them up and then adds them.
      'add': (a, b) ->
        a = @get a
        b = @get b
        @for_reals()
        a + b

      # 'rand' just resolves to a random number
      'rand': -> Math.random()

    # Request that two random numbers be added together. Actually, only one
    # random number will be created; the key 'rand' is bound to a random number,
    # and then that same number is used again.
    setup: ->
      @request 'add:rand:rand'

    # Make sure that adding two random numbers results in
    # a number between zero and two.
    expect: ->
      @db.get 'add:rand:rand', (err, str) =>
        @assert.match str, /[01]\.[0-9]+/
        @finish()


  # Test that multiple stages of @for_reals works
  'multiple resolution stages':

    workers:
      # Requests 'b' multiple times with different arguments
      a: ->
        b1 = @get 'b', 1; @for_reals()
        b2 = @get 'b', 2; @for_reals()
        b3 = @get 'b', 3; @for_reals()
        b1 + b2 + b3 # heh, these are strings :)
      b: (n) -> n

    setup: -> @request 'a'

    # Test that all the jobs ran correctly
    expect: ->
      @db.get 'a', (err, str) =>
        @assert.equal str, '"123"'
        @assert.eql @audit.messages, ['?a|b:1', '!b:1', '?a|b:2', '!b:2', '?a|b:3', '!b:3', '!a']
        @finish()
  
  
  # Test that the last @get has an implied @for_reals()
  'implicit for_reals':
  
    workers:
      a: -> @get 'b'
      b: -> 216
    
    setup: ->
      @request 'a'
    
    expect: ->
      @get 'a', (val) =>
        @assert.eql val, 216
        @finish()


  # It wouldn't be complete without a Fibonacci test!
  'fibonacci':

    workers:
      fib: (n) ->
        return 1 if n < 2
        a = @get 'fib', n-2
        b = @get 'fib', n-1
        @for_reals()
        a + b

    setup: ->
      @start_time = new Date().getTime()
      @request 'fib', 8

    # Make sure it gets the right answer. Also that it's speedy!
    expect: ->
      dt = new Date().getTime() - @start_time
      @db.get 'fib:8', (err, str) =>
        @assert.equal str, '34'
        @assert.equal true, (dt < 500)
        @finish()
