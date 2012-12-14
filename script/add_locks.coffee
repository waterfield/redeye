r = require('redis').createClient()

r.keys '*', (err, keys) ->
  hash = {}
  hash[key] = true for key in keys
  todo = []
  for key in keys
    continue if key.indexOf(':') < 0
    lock = 'lock:' + key
    continue if hash[lock]
    todo.push lock
    todo.push 'ready'
  r.mset todo, (err) ->
    throw err if err
    console.log "#{todo.length/2} locks added"
    r.end()
