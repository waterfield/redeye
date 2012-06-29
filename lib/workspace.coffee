_ = require 'underscore'
Worker = require './worker'
class Workspace
  
extend_workspace = (methods) ->
  for method, fun of methods
    do (method, fun) ->
      Workspace.prototype[method] = ->
        fun.apply Worker.current, arguments
  null

Workspace.mixin = (mixins) ->
  for method, fun of mixins
    Worker.prototype[method] ?= fun
  extend_workspace mixins

core_methods = {}
for method in ['get', 'emit', 'keys', 'worker', 'bless', 'all']
  core_methods[method] = Worker.prototype[method]
extend_workspace core_methods

Worker.Workspace = Workspace
module.exports = Workspace
