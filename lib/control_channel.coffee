consts = require './consts'
db = require './db'
_ = require 'underscore'
require './util'

module.exports = class ControlChannel
  constructor: (options) ->
    {db_index} = options
    @_db = db db_index
    @_channel = _('control').namespace db_index

  publish: (msg) -> @_db.publish @_channel, msg

  cycle: (key, deps) ->
    msg = ['cycle', key, deps...].join consts.key_sep
    @publish msg

  quit: -> @publish 'quit'

  reset: -> @publish 'reset'

  resume: (key) -> @publish "resume#{consts.key_sep}#{key}"

  delete_jobs: -> @_db.del 'jobs'

  end: -> @_db.end()

  push_job: (req) -> @_db.rpush 'jobs', req

  db: -> @_db
