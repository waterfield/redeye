redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite 

  # 'uncaught cycle test':
  #   workers:
  #     z: -> @get 'a'
  #     a: -> @get 'b'
  #     b: -> @get 'c'      
  #     c: -> 
  #       @get 'a'
  #       @emit 'q', 666
  #       setTimeout (=> @emit 'c', 216), 1500
  #   setup: ->
  #     @dispatcher.on_stuck (doc) =>
  #       @cycle ?= doc.cycles[0]
  #     @request 'z'
  #   expect: ->
  #     @assert.eql @cycle, ['a', 'b', 'c']
  #     @finish()
  # 
  # 
  'caught cycle test':
    workers:
      z: -> @get 'a'
      a: -> @get 'b'
      b: ->
        v = @get 'v'
        c = @get 'c', -> 123
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
  
  
  'double cycle test':
    workers:
      z: -> (@get('a') ? 0) + @get_now 'b'
      a: -> @get 'c'
      b: ->
        @get 'c', -> 7
      c: ->
        a = @get 'a', -> 5
        (a ? 0) + @get_now 'b'
    setup: ->
      @request 'z'
    expect: ->
      @db.mget ['z', 'a', 'b', 'c'], (err, arr) =>
        @assert.eql arr, [19, 12, 7, 12]
        @finish()
  
  
  'other double cycle test':
    workers:
      z: -> (@get('a') ? 0) + @get_now 'b'
      a: -> @get 'c'
      b: -> @get 'c'
      c: ->
        a = @get('a', -> 5) ? 0
        b = @get('b', -> 7) ? 0
        a + b
    setup: ->
      @request 'z'
    expect: ->
      @db.mget ['z', 'a', 'b', 'c'], (err, arr) =>
        @assert.eql arr, [24, 12, 12, 12]
        @finish()
  
  
  'redundant recovery':
    workers:
      a: -> @get 'b', -> 1
      b: -> @get 'c', -> 2
      c: -> @get 'a', -> 3
      z: -> (@get('a') ? 0) + (@get('b') ? 0) + (@get('c') ? 0)
    setup: ->
      @request 'z'
    expect: ->
      @get 'z', (val) =>
        @assert.eql val, 6
        @finish()
  
  
  # 'alternate call sequence':
  #   workers:
  #     a: ->
  #       @get 'b', =>
  #         @get 'b', =>
  #           @get 'c', =>
  #             @get 'd'
  #     b: -> @get 'a'
  #     c: -> @get 'a'
  #     d: -> 42
  #   setup: ->
  #     @request 'b'
  #   expect: ->
  #     @db.mget ['a', 'b', 'c', 'd'], (err, arr) =>
  #       @assert.eql arr, [42, 42, 42, 42]
  #       @finish()
