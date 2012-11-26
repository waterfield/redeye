test 'keys', ->

  worker 'a', -> _.sum @keys('x:*'), (k) => @get(k)
  setup ->
    set 'x:1', 1
    set 'x:2', 2
    set 'x:3', 5
    request 'a'
  want 8
