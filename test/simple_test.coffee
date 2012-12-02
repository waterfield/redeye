describe 'simple', ->

  test 'add', ->
    worker 'add', (a,b) -> a + b
    setup -> request 'add', 3, 4
    want 7

  test 'stages', ->
    worker 'a', -> @b(1) + @b(2) + @b(3)
    worker 'b', (x) -> x
    setup -> request 'a'
    want 6

  test 'fibonacci', ->
    worker 'fib', (n) ->
      return 1 if n < 2
      @fib(n-2) + @fib(n-1)
    setup -> request 'fib', 8
    want 34

  describe 'with has', ->

    worker 'foo', -> $.num() * $.num()
    setup -> request 'foo'

    test 'first', ->
      has num: -> 2
      want 4

    test 'second', ->
      has num: -> 3
      want 9
