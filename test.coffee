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
  @c()
  'b'

m.worker 'c', ->
  @sleep 10
  'c'

m.run()
