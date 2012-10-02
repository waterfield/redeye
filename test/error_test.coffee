_ = require 'underscore'
redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite

  'error test':

    workers:
      a: ->
        @each ->
          @get 'b'
      b: ->
        @get 'c'
        throw new Error 'asdf'
      c: -> 216

    setup: ->
      @request 'a'

    expect: ->
      @get 'a', (value) =>
        list = value.error
        @assert.ok _.isArray(list)
        @assert.eql list.length, 2
        keys = _.pluck list, 'key'
        traces = _.pluck list, 'trace'
        @assert.eql keys, ['a', 'b']
        err1 = traces[0].split("\n")[0]
        err2 = traces[1].split("\n")[0]
        @assert.eql err1, "Error: caused by dependency"
        @assert.eql err2, "Error: asdf"
        @finish()
