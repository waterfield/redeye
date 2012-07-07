MongoAdapter = require './adapter'

module.exports = class MongoKeyValue extends MongoAdapter
  get: (key, callback) ->
    @coll.findOne {key}, ['value'], (err, obj) ->
      # console.log 'get', key, '->', err, obj
      callback err, obj?.value
  get_all: (keys, callback) ->
    @coll.find({key: {$in: keys}}).toArray (err, objs) ->
      return callback(err) if err
      # console.log 'get_all', keys, '->', err, objs
      hash = {}
      hash[obj.key] = obj.value for obj in objs
      callback null, (hash[key] for key in keys)
  keys: (pattern, callback) ->
    re = new RegExp(pattern.split('*').join('.*'))
    @coll.find({key: re}, ['key']).toArray (err, objs) ->
      return callback(err) if err
      # console.log 'keys', pattern, '->', err, objs
      callback null, (obj.key for obj in objs)
  set: (key, value, callback) ->
    # console.log 'set', key
    @coll.insert {key, value}, {safe: true}, (err) ->
      callback(err) if callback
  exists: (key, callback) ->
    @get key, (err, obj) ->
      # console.log 'exists', key, '->', obj?
      callback err, obj?
  atomic_set: (key, value, callback) ->
    @coll.insert {_id: {atomic: key}, key, value}, {safe: true}, (err) =>
      if err
        @get key, callback
      else
        callback null, value
  map_reduce: (pattern, map, reduce, callback) ->
    # TODO
  del: (key, callback) ->
    @coll.remove {key}, {safe: true}, (err) ->
      callback(err) if callback
  flush: (callback) ->
    @coll.remove {}, {safe: true}, (err) ->
      callback(err) if callback
