Manager = require './lib/manager'
pool = require './lib/pool'

m = new Manager

m.worker 'foo', ->
  @all ->
    @a()
    @b()
    @c()

m.worker 'a', ->
  @sleep 1
  db = @worker().manager.db
  s = @worker().manager.scripts
  db.evalsha s.dirty, 0, 'b'
  'a'

m.worker 'b', ->
  @sleep 2
  'b'

m.worker 'c', ->
  @sleep 2
  'c'

go = ->
  pool.acquire (err, db) ->
    throw err if err
    db.flushdb ->
      db.set('lock:foo', 'queued')
      db.rpush('jobs', 'foo')
      m.run()
      setTimeout andthen, 5000

andthen = ->
  m.db.set('lock:foo', 'queued')
  m.db.rpush('jobs', 'foo')


setTimeout go, 500
