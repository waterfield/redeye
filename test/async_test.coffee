redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  'async test':

    workers:
      'test': ->
        @async (callback) ->
          setTimeout (-> callback null, 216), 100

    setup: ->
      @request 'test'

    expect: ->
      @get 'test', (value) =>
        @assert.eql value, 216
        @finish()
