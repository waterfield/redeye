{Db, Server} = require 'mongodb'

module.exports = class MongoAdapter
  constructor: (@options) ->
  connect: (callback) ->
    dbname = 'redeye' + (@options.db_index ? '')
    dbname = @options.database ? dbname
    host = @options.host ? '127.0.0.1'
    port = @options.port ? 27017
    collname = @options.collection ? 'kv'
    @db = new Db dbname, new Server(host, port, {})
    @db.open (err) =>
      throw err if err
      @db.collection collname, (err, @coll) =>
        throw err if err
        callback {@db, @coll}
  end: ->
    @db.close()
