# Test that the idle handler is called.

# Dependencies.
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
      @request 'a'

    expect: ->
      @assert.eql @dispatcher.doc.loose_ends, ['b']
      @finish()
