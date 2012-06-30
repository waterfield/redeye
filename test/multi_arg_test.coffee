redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  # Test that jobs can accept arguments
  'test of multi args':
  
    # Define some workeres with arguments. Both 'x' and 'y' do the same
    # thing: they multiply their arguments. However, 'y' shows how to do
    # this with arguments rather than a compound key.
    workers:
      x: (a, b) -> @emit @worker().key, parseInt(a) * parseInt(b)
      y: (a, b) -> @emit 'y', a, b, parseInt(a) * parseInt(b)
      
      # The 'problem' job resolves to (2 * 3) + (1 * 7). It does this
      # by requesting 'x' and 'y', with their arguments explicitly
      # written out instead of as compound keys (such as 'x:2:3').
      problem: ->
        a = @get 'x', 2, 3
        b = @get 'y', 1, 7
        a + b

    # Kick off the process
    setup: ->
      @request 'problem'

    # Just make sure the result is right
    expect: ->
      @_kv.get 'problem', (err, str) =>
        @assert.equal str, '13'
        @finish()
