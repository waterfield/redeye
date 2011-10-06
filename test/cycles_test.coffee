# Test that the idle handler is called.

redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite 

  'idle test':
  
    workers:
      a: -> @get 'b'; @for_reals()
      b: -> @get 'c'; @for_reals()
      c: -> 
        @get 'a'
        setTimeout (=> @emit 'c', 216), 1000
        @for_reals()

    setup: ->
      @request 'a'

    expect: ->
      @assert.eql @dispatcher.doc.cycles[0], ['a', 'b', 'c']
      @finish()
