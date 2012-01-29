class CycleError
  is_cycle: true
  constructor: (@key) ->

module.exports = CycleError
