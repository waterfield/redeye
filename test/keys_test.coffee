redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite 

  # Test the @keys method
  'keys test':
  
    workers:
      # 'a' gets all 'x:*', then sums them
      a: ->
        sum = 0
        sum += @get(key) ? 0 for key in @keys 'x:*'
        sum
        
    # Set the 'x:*' keys, then request a sum
    setup: ->
      @_kv.set 'x:1', '1'
      @_kv.set 'x:2', '2'
      @_kv.set 'x:3', '5'
      @request 'a'

    # Assert that the doctor ran, and that it detected
    # our cyclic dependency.
    expect: ->
      @get 'a', (sum) =>
        @assert.equal sum, 8
        @finish()
