# Toggle this to enable Redeye debug output
enable_debugging = false

module.exports =

  # Log a message to the console, if debugging is enabled.
  log: if enable_debugging then console.log else () ->
