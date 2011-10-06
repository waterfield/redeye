redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  # Test that the idle handler is called, and that the loose
  # end of 'b' is detected.
  'idle test':
  
    workers:
      # 'a' depends on 'b'
      a: -> @get 'b'
      
      # But 'b' doesn't emit itself for quite a while
      b: ->
        @emit 'c', 216
        setTimeout (=> @emit 'b', 42), 1000

    # Make the request for 'a'
    setup: ->
      @request 'a'

    # Assert that the doctor was called when 'b' was being
    # slow, and that it detected 'b' as a loose end.
    expect: ->
      @assert.eql @dispatcher.doc.loose_ends, ['b']
      @finish()
