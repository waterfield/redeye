worker = require 'worker'
worker 'rand', ->
  console.log "! rand"
  Math.random()
