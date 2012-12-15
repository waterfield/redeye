# Usage:
#
#   coffee script/run key:to:request worker_file1 ...

{ Manager } = require '..'
_ = require 'underscore'

m = new Manager max_cache_items: 100

seed = process.argv[2]

for worker_file in process.argv[3..]
  require(worker_file).init m

m.run()

m.on 'ready', ->
  console.log 'requesting', seed
  m.request seed

m.on 'quit', ->
  console.log 'quitting'

count = 0
total = 0
rate = ->
  console.log(_.extend {total_keys: total, keys_per_sec: count}, m.cache.stats)
  count = 0

timer = setInterval rate, 1000

m.on 'redeye:finish', (payload) ->
  { key } = payload
  # console.log key
  count++
  total++
  if key == seed
    m.quit()
    clearInterval timer
