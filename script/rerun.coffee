
redis = require 'redis'
Manager = require '../lib/manager'
_ = require '../lib/util'

# Print usage message and die
usage = ->
  console.log "Usage: coffee script/rerun.coffee -w <worker> [-p <port>] [-s <slice>]"
  process.exit 1

# Parse argument options
argv = require('optimist').argv
slice = argv.s ? process.env['SLICE'] ? 2
port = argv.p ? process.env['REDIS_PORT'] ? 6379
seed = argv.seed ? process.env['SEED'] ? null

usage() unless argv.w

# Create redis connection
r = redis.createClient port, 'localhost', detect_buffers: true
r.select slice

# Globals
to_delete = []
manager = null

# Iterator (sequential mode)
each = (list, final, fun) ->
  index = 0
  next = ->
    return final() if index == list.length
    fun list[index++], next
  next()

# Boot up manager with worker definitions and re-request
# seed key. Shut the script down when the seed key is done.
rerun = ->
  unless seed
    console.log "Couldn't determine seed :("
    r.end()
    return
  console.log "Seed: #{seed}"
  manager = new Manager { slice, port }
  require(argv.w).init manager
  manager.run()
  manager.on 'ready', ->
    r.publish "requests_#{slice}", '!reset'
    r.end()
    setTimeout listen_for_completion, 500
  manager.on 'quit', ->
    r.end()
    r = redis.createClient port, 'localhost', return_buffers: true
    r.select slice
    r.get seed, (err, buf) ->
      throw err if err
      value = JSON.parse(buf) if buf
      console.log 'Done. Got:', value
      r.get 'fatal', (err, fatal) ->
        throw err if err
        r.lrange 'errors', 0, 0, (err, list) ->
          throw err if err
          if fatal
            console.log "Fatal:", fatal
          else if list.length
            err = JSON.parse list[0].toString()
            console.log "An error: #{err.handle}: #{err.message}"
          else
            console.log "No errors!"
          r.end()

listen_for_completion = ->
  manager.request seed
  r = redis.createClient port, 'localhost', return_buffers: true
  r.select slice
  r.subscribe 'redeye:finish'
  r.on 'message', (c, msg) ->
    manager.quit() if JSON.parse(msg).key == seed
  # manager.on 'redeye:finish', (log) ->
  #   manager.quit() if log.key == seed

# Delete all the collected intermediate keys, then call `rerun`.
delete_keys = ->
  to_delete.push 'errors'
  if to_delete.length
    chunks = _(to_delete).in_groups_of(10000)
    wrap_up = ->
      console.log "Deleted #{(to_delete.length-1)/4} keys"
      rerun()
    each chunks, wrap_up, (chunk, next) ->
      r.del chunk..., next
  else
    console.log "No keys to delete"
    rerun()

explicit_inputs = [
  'accrual.id_to_anchor'
  'accrual.anchor_to_id'
  'accrual.id_to_id'
  'accrual.asset_calc'
  'accrual.calc_summary'
  'accrual.accrual_recovery'
  'accrual.level_analysis'
  'accrual.groups_for_meter'
  'accrual.meters_for_group'
  'accrual.nearby_meters'
  'accrual.primary_meters'
  'accrual.secondary_meters'
  'accrual.asset_contracts'
  'accrual.batch_process'
  'accrual.prior_production_date'
  'accrual.latest_accounting_date'
  'accrual.primaries_for_secondary'
  'accrual.secondary_for_primary'
  'accrual.meter_gas_types'
  'accrual.secondary_gas_types'
  'accrual.tailgate_gas_types'
  'accrual.tailgate'
  'accrual.index_scenario'
  'accrual.index_scenario_item'
  'accrual.asset_scenario'
  'accrual.asset_scenario_item'
  'accrual.ca_percent'
  'accrual.meters_for_contract'
  'accrual.code_tables'
  'accrual.volume_curves'
  'accrual.decline_curves'
  'accrual.decline_curve_items'
  'accrual.meter'
  'accrual.contract'
  'accrual.product_terms'
  'accrual.wh_term'
  'accrual.fee_terms'
  'accrual.asset'
  'accrual.forecast'
  'accrual.rate'
  'accrual.count_tailgates'
  'accrual.tailgate'
]

# Find out what keys are intermediate and what is the seed,
# then call `delete_keys`.
scan_db = ->
  r.keys 'lock:*', (err, locks) ->
    throw err if err
    each locks, delete_keys, (lock, next) ->
      [unused, prefix, rest...] = lock.split ':'
      key = [prefix, rest...].join ':'
      do (key, prefix) ->
        r.scard "targets:#{key}", (err, targets) ->
          throw err if err
          do (targets) ->
            r.scard "sources:#{key}", (err, sources) ->
              throw err if err
              if (sources && !(prefix in explicit_inputs)) || (prefix == 'one_shot_cashout')
                unless targets
                  # console.log 'Potential seed:', key, { sources, targets } # XXX
                  seed ?= key
                to_delete.push key
                to_delete.push 'lock:'+key
                to_delete.push 'sources:'+key
                to_delete.push 'targets:'+key
              next()

scan_db()
