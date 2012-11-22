Manager = require './lib/manager'
pool = require './lib/pool'

m = new Manager

m.worker 'foo', -> @all -> @bar1(); @bar2()
m.worker 'bar1', ->
  try
    @baz()
  catch err
    if err.cycle
      @quux()
    else
      throw err
m.worker 'bar2', ->
  try
    @baz()
  catch err
    if err.cycle
      @quux()
    else
      throw err
m.worker 'baz', -> @foo()
m.worker 'quux', -> 'quux'

pool.acquire (err, db) ->
  throw err if err
  db.flushdb ->
    db.set('lock:foo', 'queued')
    db.rpush('jobs', 'foo')
    m.run()
