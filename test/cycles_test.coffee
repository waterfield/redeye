redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite 

  'uncaught cycle test':
  
    workers:
      # 'a' depends on 'b', and 'b' on 'c'
      z: -> @get 'a'
      a: -> @get 'b'
      b: -> @get 'c'
      
      # 'c' is defined, but depends on 'a', creating a 
      # cyclic dependency. The setTimeout is used so that
      # the test does complete, but only after the idle
      # handler (the Doctor) has been called.
      c: -> 
        @get 'a'
        @emit 'q', 666
        setTimeout (=> @emit 'c', 216), 1000
  
    # Make a request that can't be fulfilled in time
    setup: ->
      @request 'z'
  
    # Assert that the doctor ran, and that it detected
    # our cyclic dependency.
    expect: ->
      @assert.eql @dispatcher.doc.cycles[0], ['a', 'b', 'c']
      @finish()

  'caught cycle test':
  
    workers:
      z: -> @get 'a'
      a: -> @get 'b'
      b: ->
        v = @get 'v'
        c = try
          @get 'c'
        catch e
          123
        w = @get_now 'w'
        v + c + w
      c: -> @get 'a'
      v: -> 10
      w: -> 20
    
    setup: ->
      @request 'z'
    
    expect: ->
      @assert.eql @dispatcher.doc.cycles[0], ['a', 'b', 'c']
      @db.mget ['a', 'b', 'c'], (e, arr) =>
        @assert.eql arr, [153, 153, 153]
        @finish()

  # 'double cycle test':
  #   workers:
  #     z: -> (@get('a') ? 0) + @get_now 'b'
  #     a: -> @get 'c'
  #     b: ->
  #       try
  #         @get 'c'
  #       catch e
  #         7
  #     c: ->
  #       a = try
  #         @get 'a'
  #       catch c
  #         5
  #       (a ? 0) + @get_now 'b'
  #   
  #   setup: ->
  #     @request 'z'
  #   
  #   expect: ->
  #     @db.mget ['z', 'a', 'b', 'c'], (err, arr) =>
  #       @assert.eql arr, [19, 12, 7, 12]
  #       @finish()