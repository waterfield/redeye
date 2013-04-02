sdc = require 'statsd-client'

host = process.argv['STATS_HOST'] ? '127.0.0.1'
prefix = process.argv['STATS_PREFIX'] ? 'we.dev.nathan.redeye.test'

module.exports = new sdc { host, prefix }
