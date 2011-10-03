enable_debugging = true

module.exports =
  log: if enable_debugging then console.log else () ->
