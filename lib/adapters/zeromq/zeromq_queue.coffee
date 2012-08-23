Queue = require '../../db/queue'

class ZeromqQueue extends Queue
  push: (name, value) ->
  pop: (name, callback) ->
  del: (name) ->
  end: ->
  