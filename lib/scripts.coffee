fs = require 'fs'
redis = require 'redis'

db = null

scripts = ['require', 'refresh', 'orphans', 'dirty', 'finish']
shas = {}

load_next_script = (callback) ->
  if script = scripts.shift()
    load_script script, callback
  else
    db.end()
    callback null, shas

load_script = (script, callback) ->
  path = "#{__dirname}/../lua/#{script}.lua"
  contents = fs.readFileSync path
  db.send_command 'script', ['load', contents], (err, sha) ->
    return callback(err) if err
    shas[script] = new Buffer(sha)
    load_next_script callback

exports.load = (callback) ->
  db = redis.createClient()
  load_next_script callback
