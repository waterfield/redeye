r = require('redis').createClient 6379, 'localhost', detect_buffers: true
_ = require 'underscore'

r.keys '*', (err, keys) ->
  throw err if err
  count = keys.length
  done = ->
    unless --count
      r.end()
  for key in keys
    if key.substring(0,5) == 'lock:'
      done()
      continue
    do (key) ->
      buf = new Buffer key
      r.get buf, (err, val) ->
        throw err if err
        try
          obj = msgpack.unpack val
          if _.isArray(obj?.error)
            console.log obj
        catch e
        finally
          done()
