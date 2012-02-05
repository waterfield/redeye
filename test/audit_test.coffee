redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  # Tests the audit trail produced by the dispatcher
  'test result and audit log':
  
    workers:
      # 'a' depends on 'b' and 'c'
      a: -> @get 'b'; @get 'c'
      
      # 'b' also depends on 'c'
      b: -> @get 'c'
      
      # 'c' is a placeholder
      c: -> 216

    # Kick off by requesting 'a'
    setup: ->
      @request 'a'

    # Assert that the correct dependency graph is generated in
    # the audit log. Note that there are exactly two valid
    # total orderings of the dependency graph produced above.
    expect: ->
      @db.get 'a', (err, str) =>
        @assert.eql @audit.messages, ['?a|b', '?b|c', '!c', '!b', '!a']
        @finish()
