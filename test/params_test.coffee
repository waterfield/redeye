Workspace = require '../lib/workspace'

class Subspace extends Workspace
  constructor: (@b) ->
  value: -> @x()

class Foo extends Workspace
  constructor: (@value) ->
  foo: -> @value * 2

describe 'params', ->

  worker 'x', 'a', 'b', -> @a + @b

  test 'as @ locals', ->
    setup -> request 'x', 'foo', 'bar'
    want 'foobar'

  test 'named worker via object', ->
    worker 'y', -> @x b: 'bar', a: 'foo'
    setup -> request 'y'
    want 'foobar'

  test 'local re-used for @get', ->
    worker 'y', 'a', 'b', -> @x()
    setup -> request 'y', 'foo', 'bar'
    want 'foobar'

  # NOTE: this is commented because for some reason it doesn't play well with other tests
  # test 'sub-spaces override locals', ->
  #   worker 'y', 'a', 'b', -> new Subspace('baz').value()
  #   setup -> request 'y', 'foo', 'bar'
  #   want 'foobaz'

  test 'declared type used in build', ->
    worker 'foo', as: Foo, (x) -> x
    worker 'y', -> @foo(3).foo()
    setup -> request 'y'
    want 6
