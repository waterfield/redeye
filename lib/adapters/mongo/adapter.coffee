{Db, Server} = require 'mongodb'
_uses = 0
_mongo = null

module.exports = class MongoAdapter
  constructor: (@options) ->
    {@db, @coll} = _mongo if _mongo
  connect: (callback) ->
    MongoAdapter.connect @options, ({@db, @coll}) =>
      _uses++
      callback?()
  end: ->
    unless --_uses
      @db.close()

MongoAdapter.connect = (options, callback) ->
  return callback(_mongo) if _mongo
  dbname = options.database ? 'redeye'
  host = options.host ? '127.0.0.1'
  port = options.port ? 27017
  collname = options.collection ? 'kv'
  db = new Db dbname, new Server(host, port, {})
  db.open (err) =>
    throw err if err
    db.collection collname, (err, coll) =>
      throw err if err
      callback(_mongo = {db, coll})