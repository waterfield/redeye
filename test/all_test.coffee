# Tests that @all / @each work as expected; also tests that
# the work queue is actually a stack by testing that the terminal
# key is called in reverse order from the requests.

describe 'multi', ->

  worker '_all', -> @all -> @x(1); @x(2); @x(3)
  worker '_each', -> @each -> @x(1); @x(2); @x(3)
  worker 'x', (n) -> $.req.push n; n

  has req: []

  expect ->
    assert.equal $.req, [3,2,1]

  test 'all', ->
    setup -> request '_all'
    want [1,2,3]

  test 'each', ->
    setup -> request '_each'
    want 3

test 'iterators', ->

  worker '_all', -> _.flatten @all x: [1,2,3], @foo
  worker 'foo', 'x', -> [ { @x } ]

  setup -> request '_all'
  want [{x:1},{x:2},{x:3}]
