# To create your own workers, do this:
# 
#     worker = require './worker'
#     worker 'prefix', (arg1, arg2) -> ...

# Grab reference to list of runners.
runners = require('./redeye').runners

# Export the function that adds a worker.
module.exports = (prefix, runner) ->
  runners[prefix] = runner
