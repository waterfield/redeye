describe 'simple', ->

  test 'add', ->
    worker 'add', (a,b) -> parseInt(a) + parseInt(b)
    setup -> request 'add', 3, 4
    want 7

  test 'stages', ->
    worker 'a', -> @b(1) + @b(2) + @b(3)
    worker 'b', (x) -> parseInt(x)
    setup -> request 'a'
    want 6

  test 'fibonacci', ->
    worker 'fib', (n) ->
      n = parseInt n
      return 1 if n < 2
      @fib(n-2) + @fib(n-1)
    setup -> request 'fib', 8
    want 34
