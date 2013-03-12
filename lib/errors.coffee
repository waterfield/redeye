class DependencyError extends Error
  constructor: (@worker, @tail) ->
    super
    @message = 'Caused by dependency'
    @name = 'DependencyError'
    Error.captureStackTrace @, @constructor
    trace = @stack
    { key, manager } = @worker
    { slice } = manager
    @tail.push { trace, key, slice }

  get_tail: -> @tail

  full_message: (message, context) ->
    list = [message + "\n"]
    for item in @tail
      { key, trace, slice } = item
      slice = if slice then "[#{slice}] " else ''
      list.push "In worker: #{slice}#{key}"
      for line in trace.split "\n"
        list.push "    #{line}"
    list.join("\n")

class MultiError extends Error
  constructor: (@worker, @errors = []) ->
    super
    @message = 'Multiple errors in parallel'
    @name = 'MultiError'
    Error.captureStackTrace @, @constructor

  add: (error) ->
    @errors.push error

  each: (fun) ->
    fun error for error in @errors

  _get_tail: (error) ->
    tail = error.get_tail?()
    return tail if tail
    { key, manager } = @worker
    { slice } = manager
    trace = error.stack
    [{ trace, key, slice }]

  get_tail: -> @_get_tail @errors[0]

class CycleError extends Error
  constructor: (@cycle) ->
    super
    @message = @cycle.join(' <- ')
    @name = 'CycleError'
    Error.captureStackTrace @, @constructor

  complete: ->
    @cycle[0] == @cycle[@cycle.length - 1]

  tail: ->
    [{ trace: @stack, key: @cycle[0] }]

module.exports = {
  DependencyError
  MultiError
  CycleError
}
