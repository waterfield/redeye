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
    return if @map[key]? || @sticky[key]?
    if sticky
      @stats.sticky_items++
      @sticky[key] = value
      return
    @shrink()
    item = new CacheItem key, value
    item.add_after @head
    @stats.lru_items++
    @stats.added++
    @map[key] = item

  remove: (key) ->
    if item = @map[key]
      delete @map[key]
      item.remove()
      @stats.lru_items--
      @stats.removed++
      item.value

  get: (key) ->
    if (value = @sticky[key])?
      @stats.sticky_hits++
      value
    else if item = @map[key]
      item.hits++
      @stats.lru_hits++
      unless item == @head.next
        item.remove()
        item.add_after @head
      item.value
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

module.exports = Cache
