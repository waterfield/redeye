redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  # Test that the idle handler is called.
  'idle test':
  
    workers:
      # 'a' depends on 'b'
      a: -> @get 'b'
        
      # But 'b' actually emits 'c', then takes its sweet
      # time getting around to emitting 'b'. The idle
      # handler should be called in the mean time.
      b: ->
        @emit 'c', 216
        setTimeout (=> @emit 'b', 42), 1000

    # Make the idle handler just record that idling happened
    setup: ->
      @dispatcher.on_idle => @idled = true
      @request 'a'

    # Assert that idling did in fact happen
    expect: ->
      @assert.equal @idled, true
      @finish()
