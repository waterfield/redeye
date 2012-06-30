Queue = require '../../db/queue'

class RedisQueue extends Queue
  constructor: (@db) ->
  push: (name, value) ->
    @db.rpush name, value
  pop: (name, callback) ->
    @db.blpop name, 0, (err, [k, v]) =>
      if err then @err(err) else callback(v)
  del: (name, callback) ->
    @db.del name, (err) =>
      if err then @err(err) else callback?()
  end: ->
    @db.end()
