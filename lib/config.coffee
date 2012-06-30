fs = require 'fs'
raw = fs.readFileSync "#{__dirname}/../config.json"
module.exports = JSON.parse(raw.toString())
