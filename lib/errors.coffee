class exports.DependencyError extends Error
  constructor: (@worker, @tail) ->
    super
    @message = 'Caused by dependency'
    @name = 'DependencyError'
    Error.captureStackTrace @, @constructor
    trace = @stack
    {key, slice} = @worker
    @tail.unshift { trace, key, slice }

  get_tail: -> @tail

  full_message: (message, context) ->
    list = [message + "\n"]
    for item in @tail
      { key, slice, trace } = item
      list.push "In worker: [#{slice}] #{key}"
      for line in trace.split "\n"
        list.push "    #{line}"
    list.join("\n")

class exports.MultiError extends Error
  constructor: (@errors = []) ->
    super
    @message = 'Multiple errors in parallel'
    @name = 'MultiError'
    Error.captureStackTrace @, @constructor

  add: (error) ->
    @errors.push error

  each: (fun) ->
    fun error for error in @errors

  get_tail: -> @errors[0].get_tail()
