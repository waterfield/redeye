Manager = require './lib/manager'

m = new Manager

m.worker 'foo', ->
  @all ->
    @a()
    @b()

m.worker 'a', ->
  @sleep 5
  @worker().db.rpush 'dirty', 'b'
  'a'

m.worker 'b', ->
  @sleep 10
  'b'

m.run()
