enable_debugging = false

module.exports =
  log: if enable_debugging then console.log else () ->
