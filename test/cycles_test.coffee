redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite 

  # Test that the idle handler is called.
  'cycle test':
  
    workers:
      # 'a' depends on 'b', and 'b' on 'c'
      z: -> @get 'a'
      a: -> @get 'b'
      b: -> @get 'c'
      
      # 'c' is defined, but depends on 'a', creating a 
      # cyclic dependency. The setTimeout is used so that
      # the test does complete, but only after the idle
      # handler (the Doctor) has been called.
      c: -> 
        @get 'a'
        @emit 'q', 666
        setTimeout (=> @emit 'c', 216), 1000

    # Make a request that can't be fulfilled in time
    setup: ->
      @request 'z'

    # Assert that the doctor ran, and that it detected
    # our cyclic dependency.
    expect: ->
      @assert.eql @dispatcher.doc.cycles[0], ['a', 'b', 'c']
      @finish()
