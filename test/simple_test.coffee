# Test that a simple set of jobs can work together.

redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  'simple test':
  
    workers:
      'add': (a, b) ->
        a = @get a
        b = @get b
        @for_reals()
        a + b
      
      'rand': -> Math.random()

    setup: ->
      @request 'add:rand:rand'

    expect: ->
      @db.get 'add:rand:rand', (err, str) =>
        @assert.match str, /[01]\.[0-9]+/
        @finish()

