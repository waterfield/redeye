consts = require './consts'
_ = require 'underscore'
require './util'
db = require './db'

module.exports = class ControlChannel
  constructor: (options) ->
    {db_index} = options
    @_pubsub = db.pub_sub options
    @_queue = db.queue options
    @_channel = _('control').namespace db_index

  publish: (msg) -> @_pubsub.publish @_channel, msg

  cycle: (key, deps) ->
    msg = ['cycle', key, deps...].join consts.key_sep
    @publish msg

  quit: -> @publish 'quit'

  reset: -> @publish 'reset'

  resume: (key) -> @publish "resume#{consts.key_sep}#{key}"

  erase: (key) -> @publish "erase#{consts.key_sep}#{key}"

  delete_jobs: -> @_queue.del 'jobs'

  end: ->
    @_pubsub.end()
    @_queue.end()

  push_job: (req) -> @_queue.push 'jobs', req
  