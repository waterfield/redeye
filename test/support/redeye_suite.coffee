module.exports = (fun) ->
  tests = fun()
  db = require('db')()
  for name, test of tests
    tests[name] = (exit, assert) ->
      db.flushall ->
        disp = require('dispatcher').run(true)
        require('redeye').run ->
          test.expect db, assert, -> db.end()
        setTimeout (-> test.setup db, disp), 100
  tests
