_ = require 'underscore'

class Cache

  constructor: (opts = {}) ->
    { @max_items } = opts
    unless @max_memory || @max_items
      throw new Error "Must specify some kind of size limit"
    @reset()

  reset: ->
    @sticky = {}
    @map = {}
    @head = new CacheItem
    @head.next = @head.prev = @head
    @stats =
      lru_items: 0
      sticky_items: 0
      lru_hits: 0
      sticky_hits: 0
      misses: 0
      added: 0
      removed: 0

  add: (key, value, sticky = false) ->
    return item.get() if item = (@sticky[key] || @map[key])
    item = new CacheItem key, value
    if sticky
      @stats.sticky_items++
      @sticky[key] = item
      return item.get()
    @shrink()
    item.add_after @head
    @stats.lru_items++
    @stats.added++
    @map[key] = item
    item.get()

  remove: (key) ->
    if item = @map[key]
      delete @map[key]
      item.remove()
      @stats.lru_items--
      @stats.removed++
      item.value

  get: (key) ->
    if (item = @sticky[key]) != undefined
      @stats.sticky_hits++
      item.get()
    else if item = @map[key]
      item.hits++
      @stats.lru_hits++
      unless item == @head.next
        item.remove()
        item.add_after @head
      item.get()
    else
      @stats.misses++
      undefined

  shrink: ->
    while true
      break if @max_items && (@stats.lru_items < @max_items)
      @remove @head.prev.key

  keys: ->
    next = @head.next
    while (item = next) != @head
      next = item.next
      item.key

class CacheItem

  constructor: (@key, @value) ->
    @prev = null
    @next = null
    @hits = 0

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

  get: ->
    if _.isObject(@value)
      @sub @value
    else if _.isArray(@value) && _.isObject(@value[0])
      @sub item for item in @value
    else
      @value

  sub: (parent) ->
    child = {}
    child.__proto__ = parent
    child

module.exports = Cache
