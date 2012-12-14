r = require('redis').createClient()

r.keys '*', (err, keys) ->
  hash = {}
  hash[key] = true for key in keys
  todo = []
  for key in keys
    idx = key.indexOf(':')
    continue if idx < 0
    continue if key.substring(0, idx) == 'lock'
    lock = 'lock:' + key
    continue if hash[lock]
    todo.push lock
    todo.push 'ready'
  done = ->
    console.log "#{todo.length/2} locks added"
    r.end()
  if todo.length
    r.mset todo, (err) ->
      throw err if err
      done()
  else
    done()
