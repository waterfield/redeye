# Usage:
#
#   coffee script/run key:to:request worker_file1 ...

{ Manager } = require '..'

m = new Manager

seed = process.argv[2]

for worker_file in process.argv[3..]
  require(worker_file).init m

m.run()

m.on 'ready', ->
  console.log 'requesting', seed
  m.request seed, console.log

m.on 'quit', ->
  console.log 'quitting'

m.onAny (payload) ->
  console.log @event, payload
  
  if @event == 'redeye:finish' && payload.key == seed
    m.quit()
