redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  # Tests that side effects can be required without an explicit worker
  'test result and audit log':
  
    workers:
      # 'a' depends on both 'b' and 'c'. Notice that we call `@for_reals`
      # twice; after running 'b', we know implicitly that 'c' will be satisfied.
      # But we don't want to ever publish the request for 'c', since there's
      # not a worker defined for it.
      a: ->
        b = @get 'b'; @for_reals()
        c = @get 'c'; @for_reals()        
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
      @db.get 'a', (err, str) =>
        @assert.equal str, '5'
        @assert.eql @audit.messages, ['?a|b', '!c', '!b', '!a']
        @finish()
