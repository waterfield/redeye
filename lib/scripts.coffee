fs = require 'fs'
pool = require './pool'

scripts = ['require', 'refresh', 'orphans']
shas = {}

load_next_script = (db, callback) ->
  if script = scripts.shift()
    load_script script, db, callback
  else
    pool.release db
    callback null, shas

load_script = (script, db, callback) ->
  path = "#{__dirname}/../lua/#{script}.lua"
  contents = fs.readFileSync path
  db.send_command 'script', ['load', contents], (err, sha) ->
    throw new Error err if err
    shas[script] = new Buffer(sha)
    load_next_script db, callback

exports.load = (callback) ->
  pool.acquire (err, db) ->
    return callback err if err
    load_next_script db, callback
