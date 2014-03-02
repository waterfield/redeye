_ = require 'underscore'

int_re = /^\d+$/

_.mixin

  compact: (list) ->
    _(list).select (item) -> item?

  opts: (args) ->
    if !args.length
      {}
    else if args[args.length - 1]?.__proto__ == ({}).__proto__
      args.pop()
    else
      {}

  standardize_args: (args) ->
    for arg, index in args
      if int_re.test(arg)
        args[index] = parseInt(arg)
      else if arg == ''
        args[index] = null

  namespace: (str, ns) ->
    if ns then "#{str}_#{ns}" else str

  none: (arr) ->
    for elem in arr
      return false if elem
    true

  sort: (arr) -> arr.sort()

  gsub: (str, re, repl) ->
    re = new RegExp(re) if _.isString(re)
    out = ''
    while m = re.exec str
      out += str.slice(0, m.index)
      out += m[0].replace re, repl
      str = str.slice(m.index + m[0].length)
    out + str

  callback: (args) ->
    if typeof(args[args.length - 1]) == 'function'
      args.pop()
    else
      null

  sum: (list, f) ->
    add = (m, x) -> m + (f?(x) ? x ? 0)
    _.reduce list, add, 0

  in_groups_of: (list, n, f) ->
    groups = []
    sub = []
    for item in list
      sub.push item
      if sub.length == n
        groups.push sub
        sub = []
    if sub.length
      groups.push sub
    if typeof(f) == 'function'
      _.each groups, f
    groups

  without: (arr, elem) ->
    idx = arr.indexOf elem
    if idx < 0
      arr
    else
      a = arr[0...idx]
      b = arr[idx+1..-1]
      [a..., b...]

module.exports = _
