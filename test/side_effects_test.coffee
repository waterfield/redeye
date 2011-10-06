# Tests that side effects can be required withotu an explicit worker

redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  'test result and audit log':
  
    workers:
      a: ->
        b = @get 'b'; @for_reals()
        c = @get 'c'; @for_reals()
        b + c
      
      b: ->
        @emit 'c', 3
        @emit 'b', 2

    setup: ->
      @request 'a'

    expect: ->
      @db.get 'a', (err, str) =>
        @assert.equal str, '5'
        @assert.eql @audit.messages, ['?a|b', '!c', '!b', '!a']
        @finish()
