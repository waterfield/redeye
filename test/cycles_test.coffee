redeye_suite = require './support/redeye_suite'

module.exports = redeye_suite 

  'uncaught cycle test':
    workers:
      z: -> @get 'a'
      a: -> @get 'b'
      b: -> @get 'c'
      c: -> 
        @emit 'q', 666
        worker = @worker()
        setTimeout (-> worker.emit 'c', 216), 1500
        @get 'a'
    setup: ->
      @queue.wacky = true
      @dispatcher.wacky = true
      @dispatcher.on_stuck (doc) =>
        @cycle ?= doc.cycles[0]
      @request 'z'
    expect: ->
      @assert.eql @cycle, ['a', 'b', 'c']
      @finish()
    
  'caught cycle test':
    workers:
      z: -> @get 'a'
      a: -> @get 'b'
      b: ->
        v = @get 'v'
        c = @get 'c', -> 123
        w = @get 'w'
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
    