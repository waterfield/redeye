# Tests that side effects can be required withotu an explicit worker

# Dependencies.
worker = require 'worker'
dispatcher = require 'dispatcher'
debug = require 'debug'
redeye_suite = require './support/redeye_suite'
AuditListener = require './support/audit_listener'

audit = new AuditListener
dispatcher.audit audit

worker 'a', ->
  b = @get 'b'
  @for_reals()
  c = @get 'c'
  @for_reals()
  b + c

worker 'b', ->
  @emit 'c', 3
  @emit 'b', 2

module.exports = redeye_suite ->

  'test result and audit log':

    setup: (db) -> db.publish 'requests', 'a'

    expect: (db, assert, finish) ->
      db.get 'a', (err, str) ->
        assert.equal str, '5'
        assert.eql audit.messages, ['?a|b', '!c', '!b', '!a']
        finish()
