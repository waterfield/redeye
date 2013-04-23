sdc = require 'statsd-client'

host = process.argv['STATS_HOSTNAME'] ? '10.1.28.204'
prefix = process.argv['STATS_PREFIX'] ? 'we.dev.redeye'

module.exports = new sdc { host, prefix }
