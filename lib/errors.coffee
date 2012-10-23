class exports.DependencyError extends Error
  constructor: (@tail, @index) ->
    super
    @message = 'Caused by dependency'
    @name = 'DependencyError'
    Error.captureStackTrace @, @constructor

  get_tail: -> @tail

class exports.MultiError extends Error
  constructor: (@errors = []) ->
    super
    @message = 'Multiple errors in parallel'
    @name = 'MultiError'
    Error.captureStackTrace @, @constructor

  add: (error, index) ->
    error.index = index
    @errors.push error

  get_tail: -> @errors[0].get_tail()
