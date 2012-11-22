Manager = require './lib/manager'
pool = require './lib/pool'

m = new Manager

m.worker 'a', -> 'a'
m.worker 'b', -> 'b'
m.worker 'foo', -> @all -> @a(); @b()

pool.acquire (err, db) ->
  throw err if err
  db.flushdb ->
    db.rpush('lock:a', 'queued')
    db.rpush('jobs', 'a')
    m.run()
    setTimeout (->
      db.set('lock:foo', 'queued')
      db.rpush('jobs', 'foo')
    ), 1000
