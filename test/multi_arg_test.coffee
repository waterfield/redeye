# Test that jobs can accept arguments

redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  'test of multi args':
  
    workers:
      x: (a, b) -> @emit @key, parseInt(a) * parseInt(b)
      y: (a, b) -> @emit 'y', a, b, parseInt(a) * parseInt(b)
      
      problem: ->
        a = @get 'x', 2, 3
        b = @get 'y', 1, 7
        @for_reals()
        a + b

    setup: ->
      @request 'problem'

    expect: ->
      @db.get 'problem', (err, str) =>
        @assert.equal str, '13'
        @finish()
