_ = require 'underscore'
Worker = require './worker'

class Workspace

  @_anchor: {}
  @_choose: (w) -> Workspace._anchor.__proto__ = w

  mutate: (info...) ->
    Workspace.mutate @, info...

  # constructor: ->
  #   return if (p = @__proto__).meta or p == Workspace.prototype
  #   list = ((p = p.__proto__) while p.__proto__ != Workspace.prototype)
  #   meta = (@__proto__.meta = {__proto__: Workspace._anchor})
  #   meta[n] = f for own n, f of p for p in list.reverse()
  #   @__proto__.__proto__ = meta

  workspace: ->
    Worker.current.workspace

  get: (prefix, args...) ->
    [args, obj, opts, callback] = parse_args args
    ns = namespace(opts)
    prefix = "#{ns}.#{prefix}" if ns
    manager = Worker.current.manager
    params = manager.params[prefix]
    obj = {} if params && !obj && !args.length
    if obj
      throw new Error "No parameters defined for '#{prefix}'" unless params
      root = Worker.current.workspace
      args = for param in params
        if typeof(param) == 'object'
          continue
        else if param of obj
          obj[param]
        else if param of this
          this[param]
        else if root.hasOwnProperty param
          root[param]
        else
          throw new Error "Can't determine parameter '#{param}' for '#{prefix}'"
    opts.namespace = null
    args.push opts
    args.push callback if callback
    Worker.current.get prefix, args...

  toString: ->
    "<Workspace: #{@worker().prefix}>"

opt_names = ['sticky', 'as', 'namespace', 'str'] # str is for MyDate...

parse_args = (args) ->
  callback = _.callback args
  if typeof(args[0]) != 'object'
    opts = _.opts args
  else if args.length > 1
    opts = _.opts args
    obj = args[0]
  else if args.length == 1
    for name in opt_names
      if name of args[0]
        opts = args.shift()
        break
    unless opts
      obj = args.shift()
      opts = {}
  else
    opts = {}
  [args, obj, opts, callback]

namespace = (opts) ->
  ns = opts.namespace
  if ns == undefined
    manager = Worker.current.manager
    opts = manager.opts[Worker.current.prefix]
    ns = opts.namespace
  ns

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

Workspace.mutate = (obj, info...) ->
  if obj._mutated?
    console.log "MUTANT! #{Worker.current.key}:", info..., obj
  obj._mutated = true

core_methods = {}
for method in ['emit', 'keys', 'worker', 'bless', 'all', 'each', 'atomic', 'async', 'with', 'log', 'sleep']
  core_methods[method] = Worker.prototype[method]
extend_workspace core_methods

Worker.Workspace = Workspace
module.exports = Workspace
