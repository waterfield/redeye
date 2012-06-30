raw = File.readSync "#{__dirname__}/../config.json"
module.exports = JSON.parse(raw)
