_ = require './util'
stats = require('./stats').getChildClient('cache')

class Cache

  constructor: (opts = {}) ->
    { @max_items } = opts
    @lru_size = 0
    @sticky_size = 0
    unless @max_memory || @max_items
      throw new Error "Must specify some kind of size limit"
    @reset()

  reset: ->
    @sticky = {}
    @map = {}
    @head = new CacheItem
    @head.next = @head.prev = @head

  add: (key, value, sticky = false) ->
    return item.get() if item = (@sticky[key] || @map[key])
    item = new CacheItem key, value
    stats.increment 'add'
    if sticky
      @sticky[key] = item
      @sticky_size++
      return item.get()
    @shrink()
    @lru_size++
    item.add_after @head
    @map[key] = item
    @gauge_size()
    item.get()

  remove: (key) ->
    if item = @map[key]
      delete @map[key]
      item.remove()
      stats.increment 'remove'
      @lru_size--
      @gauge_size()
      item.value

  get: (key) ->
    if (item = @sticky[key]) != undefined
      stats.increment 'hit'
      item.get()
    else if item = @map[key]
      item.hits++
      stats.increment 'hit'
      unless item == @head.next
        item.remove()
        item.add_after @head
      item.get()
    else
      stats.increment 'miss'
      undefined

  shrink: ->
    while true
      break if @max_items && (@lru_size < @max_items)
      @remove @head.prev.key

  keys: ->
    next = @head.next
    while (item = next) != @head
      next = item.next
      item.key

  gauge_size: ->
    stats.gauge 'size.lru', @lru_size
    stats.gauge 'size.sticky', @sticky_size

class CacheItem

  constructor: (@key, @value) ->
    @prev = null
    @next = null
    @hits = 0
    @deep_freeze @value

  deep_freeze: (obj) ->
    return unless obj
    return unless typeof(obj) == 'object'
    return if Object.isFrozen(obj)
    Object.freeze obj
    @deep_freeze v for own k, v of obj

  remove: ->
    @prev.next = @next
    @next.prev = @prev
    @prev = null
    @next = null

  add_after: (item) ->
    @prev = item
    @next = item.next
    @next.prev = this
    item.next = this

  get: (value = @value) ->
    if !value || _.isString(value)
      value
    else if _.isArray(value)
      @get(item) for item in value
    else if _.isObject(value)
      @sub value
    else
      value

  sub: (parent) ->
    child = {}
    child.__proto__ = parent
    child

module.exports = Cache
