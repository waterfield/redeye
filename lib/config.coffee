fs = require 'fs'
_ = require 'underscore'
raw = fs.readFileSync "#{__dirname}/../config.json"
module.exports = JSON.parse(raw.toString())
exports.provide = (settings) ->
  _.extend exports, settings
