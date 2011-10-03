# Tests the audit trail produced by the dispatcher

# Dependencies.
worker = require 'worker'
dispatcher = require 'dispatcher'
debug = require 'debug'
redeye_suite = require './support/redeye_suite'

class AuditListener
  constructor: -> @clear()
  write: (str) -> @messages.push str.trim()
  clear: -> @messages = []

audit = new AuditListener
dispatcher.audit audit

worker 'a', -> @get 'b'
worker 'b', -> @get 'c'
worker 'c', -> 216

module.exports = redeye_suite ->

  'test result and audit log':

    setup: (db) -> db.publish 'requests', 'a'

    expect: (db, assert, finish) ->
      db.get 'a', (err, str) ->
        assert.equal str, '216'
        assert.eql audit.messages, ['?a|b', '?b|c', '!c', '!b', '!a']
        finish()
