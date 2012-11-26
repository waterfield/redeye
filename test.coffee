Manager = require './lib/manager'
pool = require './lib/pool'

max = 5000

m = new Manager flush: true

m.worker 'top', ->
  for i in [1..max]
    @get 'load', i

m.worker 'load', (i) ->
  console.log 'DONE' if i == ('' + max)
  @worker().load = [1..1000]
  'ok'

m.run ->
  console.log 'QUIT'

setTimeout (-> m.request 'top'), 500
