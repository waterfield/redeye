r = require('redis').createClient 6379, 'localhost', detect_buffers: true

r.keys '*', (err, all_keys) ->
  throw err if err
  keys = []
  for key in all_keys
    parts = key.split ':'
    continue if (parts.length == 1) || (parts[0] == 'lock')
    keys.push key
  count = keys.length
  for key in keys
    do (key) ->
      r.get key, (err, val) ->
        throw err if err
        buf = JSON.stringify(val ? null)
        r.set key, buf, (err) ->
          throw err if err
          unless --count
            console.log "Packed #{keys.length} keys"
            r.end()
