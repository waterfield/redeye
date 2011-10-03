# A handy way to collect audit output.
class AuditListener
  constructor: -> @clear()
  write: (str) -> @messages.push str.trim()
  clear: -> @messages = []

module.exports = AuditListener
