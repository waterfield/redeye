# Tests the audit trail produced by the dispatcher

redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  'test result and audit log':
  
    workers:
      a: -> @get 'b'; @get 'c'
      b: -> @get 'c'
      c: -> 216

    setup: ->
      @request 'a'

    expect: ->
      @db.get 'a', (err, str) =>
        order1 = ['?a|b|c', '?b|c', '!c', '!b', '!a'].join ''
        order2 = ['?a|b|c', '!c', '?b|c', '!b', '!a'].join ''
        real_order = @audit.messages.join ''
        @assert.includes [order1, order2], real_order
        @finish()
