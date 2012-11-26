test 'atomic', ->

  worker 'foo', ->
    baz = @atomic 'baz', 123
    bar = @atomic 'bar', 216
    baz = @atomic 'baz', 666
    [bar, baz]

  setup -> request 'foo'
  want ['216', '123']
