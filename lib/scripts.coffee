fs = require 'fs'
redis = require 'redis'

scripts = ['require', 'refresh', 'orphans', 'dirty', 'finish']
shas = {}

load_next_script = (db, callback) ->
  if script = scripts.shift()
    load_script db, script, callback
  else
    callback null, shas

load_script = (db, script, callback) ->
  path = "#{__dirname}/../lua/#{script}.lua"
  contents = fs.readFileSync path
  db.send_command 'script', ['load', contents], (err, sha) ->
    return callback(err) if err
    shas[script] = new Buffer(sha)
    load_next_script db, callback

exports.load = (db, callback) ->
  load_next_script db, callback
