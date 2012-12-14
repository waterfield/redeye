class Cache

  constructor: (opts = {}) ->
    { @max_items } = opts
    unless @max_memory || @max_items
      throw new Error "Must specify some kind of size limit"
    @reset()

  reset: ->
    @map = {}
    @head = new CacheItem
    @head.next = @head.prev = @head
    @items = 0

  add: (key, value) ->
    return if @map[key]
    @shrink()
    item = new CacheItem key, value
    item.add_after @head
    @items++
    @map[key] = item

  remove: (key) ->
    if item = @map[key]
      delete @map[key]
      item.remove()
      @items--
      item.value

  get: (key) ->
    if item = @map[key]
      unless item == @head.next
        item.remove()
        item.add_after @head
      item.value
    else
      undefined

  shrink: ->
    while true
      break if @max_items && (@items < @max_items)
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

module.exports = Cache
