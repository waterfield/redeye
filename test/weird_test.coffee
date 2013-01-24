test 'weirdness', ->
  worker 'top', ->
    _.flatten @all x: ['foo'], -> @y()
  worker 'y', 'x', -> []
  setup -> request 'top'
  want []
