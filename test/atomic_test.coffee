redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite 

  'test atomically setting keys':
  
    workers:
      foo: ->
        @worker()._kv.set 'baz', 123
        bar = @atomic 'bar', 216 # succeeds
        baz = @atomic 'baz', 666 # fails
        [bar, baz]
        
    setup: ->
      @request 'foo'

    expect: ->
      @get @requested, (value) =>
        @assert.eql value, [216, 123]
        @finish()
