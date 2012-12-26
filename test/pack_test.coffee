test 'pack', ->

  worker 'x', pack: ['a', 'b'], ->
    @y()

  worker 'y', ->
    obj = @z()
    obj.a = 3
    obj

  worker 'z', pack: ['a', 'b', 'c'], ->
    { a: 1, b: 2, c: 3 }

  setup -> request 'x'

  expect ->
    wanted 'x', a: 3, b: 2
    wanted 'y', a: 3, b: 2, c: 3
    wanted 'z', a: 1, b: 2, c: 3
