_ = require 'underscore'

_.mixin

  opts: (args) ->
    if args[args.length - 1]?.__proto__ == ({}).__proto__
      args.pop()
    else
      {}

  namespace: (str, ns) ->
    if ns then "#{str}_#{ns}" else str

  none: (arr) ->
    for elem in arr
      return false if elem
    true

  callback: (args) ->
    if typeof(args[args.length - 1]) == 'function'
      args.pop()
    else
      null

  without: (arr, elem) ->
    idx = arr.indexOf elem
    if idx < 0
      arr
    else
      a = arr[0...idx]
      b = arr[idx+1..-1]
      [a..., b...]

  timestamp: ->
    date = new Date
    y = '' + date.getFullYear()
    m = '' + (date.getMonth() + 1)
    d = '' + date.getDate()
    h = '' + date.getHours()
    min = '' + date.getMinutes()
    s = '' + date.getSeconds()
    m = '0' + m if m.length < 2
    d = '0' + d if d.length < 2
    h = '0' + h if h.length < 2
    min = '0' + min if min.length < 2
    s = '0' + s if s.length < 2
    "#{y}#{m}#{d}#{h}#{min}#{s}"

module.exports = _
