redeye_suite = require './support/redeye_suite'

gets1 = []
value1 = null
gets2 = []
value2 = null

module.exports = redeye_suite

  'test key invalidation':
    workers:
      abc1: -> @get 'abc2'
      abc2: -> gets1.push value1

    setup: ->
      step1 = =>
        value1 = 3
        @request '_|abc1'
      step2 = =>
        value1 = 5
        @request '!invalidate|*b*2' # we invalidate abc2, which invalidates abc1 as well
      step3 = =>
        @request 'abc1'

      setTimeout step1, 0
      setTimeout step2, 500
      setTimeout step3, 1000

    expect: ->
      @assert.eql gets1, [3, 5]
      @finish()

  'test that invalidation is limited in scope':  
    workers:
      abc1: -> @get 'abc2'
      abc2: -> gets2.push value2

    setup: ->
      step1 = =>
        value2 = 3
        @request '_|abc1'
      step2 = =>
        value2 = 5
        @request '!invalidate|*b*1' # we invalidate ONLY abc1; abc2 remains complete
      step3 = =>
        @request 'abc1'

      setTimeout step1, 0
      setTimeout step2, 500
      setTimeout step3, 1000

    expect: ->
      @assert.eql gets2, [3]
      @finish()