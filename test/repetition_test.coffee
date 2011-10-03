# Tests that multiple requests are just satisfied once

# Dependencies.
worker = require 'worker'
dispatcher = require 'dispatcher'
debug = require 'debug'
redeye_suite = require './support/redeye_suite'
AuditListener = require './support/audit_listener'

audit = new AuditListener
dispatcher.audit audit

worker 'a', -> @get 'b' for i in [1..3]
worker 'b', -> 216

module.exports = redeye_suite ->

  'test result and audit log':

    setup: (db) -> db.publish 'requests', 'a'

    expect: (db, assert, finish) ->
      assert.eql audit.messages, ['?a|b|b|b', '!b', '!a']
      finish()
