redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  # Tests that multiple requests are just satisfied once
  'test repeated key requests':
  
    workers:
      # Job 'a' has 3 separate requirements on 'b'
      a: -> @get 'b' for i in [1..3]
      b: -> 216

    # Make request to 'a', resulting in three dependencies on 'b'
    setup: ->
      @request 'a'

    # Use the auditor to make sure 'b' was only run once.
    expect: ->
      @assert.eql @audit.messages, ['?a|b', '!b', '!a']
      @finish()
