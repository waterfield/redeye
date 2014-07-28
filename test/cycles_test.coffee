describe 'cycles', ->

  worker 'a', -> @b()
  worker 'b', -> @c()
  worker 'c', -> @a()

  setup -> request 'a'

  test 'uncaught', ->
    expect ->
      msg = get('a').error?[0]?.trace?.split("\n")?[0]
      assert.equal msg, 'CycleError: a <- b <- c'
