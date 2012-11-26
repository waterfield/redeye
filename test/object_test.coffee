Workspace = require '../lib/workspace'

class SomeObj extends Workspace
  constructor: ({@value}) ->
  do_stuff: -> @value * 2
  baz: -> (new OtherObj).do_stuff()

class OtherObj extends Workspace
  do_stuff: -> @quux()

test 'objects', ->

  worker 'foo', -> @bar().do_stuff()
  worker 'bar', as: SomeObj, -> value: 7
  worker 'baz', -> @bar().baz()
  worker 'quux', -> 216
  worker '_all', (keys...) -> @all -> @get(k) for k in keys

  setup -> request '_all', 'foo', 'baz'

  expect ->
    wanted 'foo', 14
    wanted 'bar', value: 7
    wanted 'baz', 216
    wanted 'quux', 216
