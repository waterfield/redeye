_ = require 'underscore'

_.mixin
  
  opts: (args) ->
    if args[args.length - 1]?.__proto__ == ({}).__proto__
      args.pop()
    else
      {}

  namespace: (str, ns) ->
    if ns then "#{str}_#{ns}" else str