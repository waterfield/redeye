redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  # Tests that side effects can be required without an explicit worker
  'test emitted side effects':
  
    workers:
      # 'a' depends on both 'b' and 'c'.
      a: ->
        b = @get 'b'
        c = @get 'c'
        b + c
      
      # But there's only a worker defined for 'b'!
      # That's OK, since 'b' actually produces both values.
      b: ->
        @emit 'c', 3
        @emit 'b', 2

    setup: ->
      @request 'a'

    # Make sure that 'a' can be satisfied by 'b' alone, and resolves
    # to the correct value.
    expect: ->
      @_kv.get 'a', (err, str) =>
        @assert.equal str, '5'
        @assert.eql @audit.messages, ['?a|b', '!c', '!b', '!a']
        @finish()
