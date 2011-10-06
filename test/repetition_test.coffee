# Tests that multiple requests are just satisfied once

# Dependencies.
redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  'test result and audit log':
  
    workers:
      a: -> @get 'b' for i in [1..3]
      b: -> 216

    setup: ->
      @request 'a'

    expect: ->
      @assert.eql @audit.messages, ['?a|b|b|b', '!b', '!a']
      @finish()
