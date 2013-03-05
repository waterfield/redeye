describe 'namespaces', ->

  test 'inner-calling', ->
    worker 'foo', namespace: 'a', -> 216
    worker 'bar', namespace: 'a', -> @get 'foo'
    setup -> request 'a.bar'
    want 216

  test 'cross-calling', ->
    worker 'foo', namespace: 'a', -> 216
    worker 'bar', namespace: 'b', -> @get 'foo', namespace: 'a'
    setup -> request 'b.bar'
    want 216

  test 'accessors', ->
    worker 'foo', namespace: 'a', -> 2 + @bar()
    worker 'bar', namespace: 'a', -> 4
    worker 'foo', namespace: 'b', -> 3
    worker 'bar', namespace: 'b', -> @foo() + @foo(namespace: 'a')
    setup -> request 'b.bar'
    want 9
