test 'errors', ->

  worker 'a', -> @each -> @b()
  worker 'b', -> @c(); throw new Error 'asdf'
  worker 'c', -> 216

  setup -> request 'a'

  expect ->
    value = get 'a'
    list = value.error
    assert.that _.isArray(list)
    assert.equal list.length, 2
    keys = _.pluck list, 'key'
    traces = _.pluck list, 'trace'
    assert.equal keys, ['b', 'a']
    err1 = traces[0].split("\n")[0]
    err2 = traces[1].split("\n")[0]
    assert.equal err1, "Error: asdf"
    assert.equal err2, "DependencyError: Caused by dependency"
