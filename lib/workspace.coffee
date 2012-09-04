_ = require 'underscore'
Worker = require './worker'

class Workspace
  workspace: ->
    Worker.current.workspace

  get: (prefix, args...) ->
    # if we provided no arguments, only assume it's a new-style
    # key if the worker defines its parameters
    callback = _.callback args
    obj = args[0]
    queue = Worker.current.queue
    obj = {} if (!args.length) && queue.params_for(prefix)
    if obj && (typeof(obj) == 'object') && !('str' of obj || 'as' of obj || 'sticky' of obj)
      unless params = queue.params_for prefix
        throw new Error "No parameters defined for '#{prefix}'"
      root = Worker.current.workspace
      args = for param in params
        if obj[param]?
          obj[param]
        else if @hasOwnProperty param
          @[param]
        else if root.hasOwnProperty param
          root[param]
        else
          throw new Error "Can't determine parameter '#{param}' for '#{prefix}'"
    args.push callback if callback
    Worker.current.get prefix, args...

  toString: ->
    "<Workspace: #{@worker().prefix}>"

extend_workspace = (methods) ->
  for method, fun of methods
    do (method, fun) ->
      Workspace.prototype[method] = ->
        fun.apply Worker.current, arguments
  null

Workspace.mixin = (mixins) ->
  for method, fun of mixins
    delete mixins[method] if Worker.prototype[method]
  _.extend Worker.prototype, mixins
  extend_workspace mixins

core_methods = {}
for method in ['emit', 'keys', 'worker', 'bless', 'all', 'each', 'atomic']
  core_methods[method] = Worker.prototype[method]
extend_workspace core_methods

Worker.Workspace = Workspace
module.exports = Workspace
