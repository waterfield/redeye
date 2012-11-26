test 'repeat', ->

  worker 'a', -> @b() for i in [1..3]
  worker 'b', -> $.req.push 'b'; 216

  has req: []

  setup -> request 'a'

  expect ->
    assert.equal $.req, ['b']
