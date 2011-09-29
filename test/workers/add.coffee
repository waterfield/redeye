worker = require 'worker'
worker 'add', (a, b) ->
  console.log "! add: a:", a, "b:", b
  a = @get a
  b = @get b
  console.log "before, a:", a, "b:", b
  @for_reals()
  console.log "after, a:", a, "b:", b  
  a + b
