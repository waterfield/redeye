# Usage:
#
#   coffee script/run key:to:request worker_file1 ...

{ Manager } = require '..'
_ = require 'underscore'

slice = process.env['SLICE'] ? 2

m = new Manager max_cache_items: 100, slice: slice, flush: true

seed = process.argv[2]

for worker_file in process.argv[3..]
  console.log 'loading', worker_file
  require(worker_file).init m

m.run()

m.on 'ready', ->
  console.log 'requesting', seed
  m.request seed

m.on 'quit', ->
  console.log 'quitting'

count = 0
total = 0
inactive = 0
diagnosed = false

rate = ->
  if count
    inactive = 0
  else
    inactive++
  if (inactive > 3) && !diagnosed
    console.log m.diagnostic()
    diagnosed = true
  { lru_hits, sticky_hits, misses } = m.cache.stats
  ratio = (lru_hits + sticky_hits) / misses
  ratio = ('' + ratio).substring(0, 4)
  console.log count, total, ratio, inactive
  count = 0

timer = setInterval rate, 1000

m.on 'redeye:finish', (payload) ->
  { key } = payload
  console.log key
  count++
  total++
  if key == seed
    m.quit()
    clearInterval timer
