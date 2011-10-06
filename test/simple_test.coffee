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

