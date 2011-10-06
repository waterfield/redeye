# Test that the idle handler is called.

redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  'idle test':
  
    workers:
      a: ->
        @get 'b'
        @for_reals()
        
      b: ->
        @emit 'c', 216
        setTimeout (=> @emit 'b', 42), 1000

    setup: ->
      @dispatcher.when_idle => @idled = true
      @request 'a'

    expect: ->
      @assert.equal @idled, true
      @finish()
