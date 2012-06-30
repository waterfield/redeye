module.exports = class Adapter
  err: (error)
    for callback in @_error_callbacks ? []
      callback(error)
  error: (callback) ->
    (@_error_callbacks ?= []).push callback
