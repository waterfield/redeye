worker 'add', (a, b) ->
  a = @get a
  b = @get b
  @for_reals()
  a + b
