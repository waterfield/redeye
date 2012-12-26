describe 'cycles', ->

  worker 'a', -> @b()
  worker 'b', ->
    try
      @c()
    catch e
      if $.catch
        @d()
      else
        throw e
  worker 'c', -> @a()
  worker 'd', -> 216

  setup -> request 'a'

  test 'uncaught', ->
    has catch: false
    expect ->
      msg = get('a').error[0].trace.split("\n")[0]
      assert.equal msg, 'CycleError: a <- b <- c <- a'

  test 'caught', ->
    has catch: true
    want 216

