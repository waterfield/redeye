db = require './db'
_ = require './util'

module.exports = class Task

  constructor: (@options) ->
    @_kv = db.key_value @options

  connect: (callback) ->
    @_kv.connect callback

  # cleanup procedure
  #   find keys which are done, but which have no targets
  #     if none, return
  #     delete them and their state
  #     get/delete their sources
  #     remove key from each source's target
  #   repeat
  cleanup: ->
    # TODO

  # mark key as seed requirement, then push
  require: (key) ->


  # remove the 'seed' target from the given key,
  # then run a cleanup
  unrequire: (key) ->
    @_kv.sdel '_targets:'+key, 'seed'
    @cleanup()
