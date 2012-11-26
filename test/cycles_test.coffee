describe 'cycles', ->

  worker 'a', -> @b()
  worker 'b', -> @c()
  worker 'c', -> @a()
  worker 'd', -> 216

  test 'uncaught', ->
    setup -> request 'a'
    expect ->
      msg = get('a').error[0].trace.split("\n")[0]
      assert.equal msg, 'CycleError: a <- b <- c <- a'

  test 'caught', ->
    worker 'b', ->
      try
        @c()
      catch err
        @d()
    setup -> request 'a'
    want 216

