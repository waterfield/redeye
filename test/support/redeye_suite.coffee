module.exports = (fun) ->
  tests = fun()
  db = require('db')()
  for name, test of tests
    tests[name] = (exit, assert) ->
      db.flushall ->
        require('dispatcher').run(true)
        require('redeye').run ->
          test.expect db, assert, -> db.end()
        setTimeout (-> test.setup db), 100
  tests
