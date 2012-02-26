redeye_suite = require './support/redeye_suite'

gets = []

module.exports = redeye_suite

  'test key invalidation':
  
    workers:
      abc: -> @get 'xyz'
      xyz: ->
        gets.push @get_now('w')
        'OK'

    # Kick off by requesting 'a'
    setup: ->
      @set 'w', 3
      @request 'z|abc'
      next = =>
        @set 'w', 5
        @request '!invalidate|a*c'
        setTimeout (=> @request 'abc'), 500
      setTimeout next, 500

    # Assert that the sticky tests are set to the right values.
    expect: ->
      @get 'abc', (val) =>
        @assert.eql val, 'OK'
        @assert.eql gets, [3, 5]
        @finish()
