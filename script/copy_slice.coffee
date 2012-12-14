redis = require 'redis'

if process.argv.length < 4
  console.log "Usage: coffee script/copy_slice from_db to_db"
  process.exit 1

rf = redis.createClient()
rf.select parseInt(process.argv[2])

rt = redis.createClient()
rt.select parseInt(process.argv[3])

rf.keys '*', (err, keys) ->
  throw err if err
  count = keys.length
  for key in keys
    do (key) ->
      rf.get key, (err, val) ->
        throw err if err
        rt.set key, val, (err) ->
          throw err if err
          unless --count
            console.log "#{keys.length} keys moved"
            rf.end()
            rt.end()
