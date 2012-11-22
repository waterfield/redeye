msgpack = require 'msgpack'
uuid = require 'node-uuid'
{CycleError} = require './errors'
Workspace = require './workspace'
Worker = require './worker'
pool = require './pool'
util = require 'util'
scripts = require './scripts'
_ = require './util'

class Manager

  constructor: (opts={}) ->
    @id = uuid.v1()
    @workers = {}
    @mixins = {}
    @runners = {}
    @queues = opts.queues ? ['jobs']
    @params = {}
    @is_input = {}
    @as = {}
    @listeners = {}
    @task_intervals = []
    @triggers = {}
    @pending = []

  connect: (callback) ->
    scripts.load (err1, @scripts) =>
      pool.acquire (err2, @pop) =>
        pool.acquire (err3, @sub) =>
          pool.acquire (err4, @db) =>
            callback?(err1 || err2 || err3 || err4)

  listen: ->
    @sub.on 'message', (channel, msg) => @perform msg
    @sub.subscribe 'control'
    @pop_next()

  pop_next: ->
    @pop.blpop 'dirty', @queues..., 0, (err, response) =>
      return @error err if err
      [type, value] = response
      if type == 'dirty'
        @dirty value
      else
        @job type, value

  require: (queue, sources, target, callback) ->
    @db.evalsha @scripts.require, 0, queue, target, sources..., (err, arr) =>
      return @error err if err
      if arr.shift().toString() == 'cycle'
        source = arr[0].toString() # NOTE: there may be more!
        err = new CycleError source, target
        return callback err
      values = for buf in arr
        msgpack.unpack(buf) if buf
      for source in sources
        @log null, 'redeye:require', { source, target }
      callback null, values

  cycle: (key, err) ->
    # TODO
    delete @workers[key]

  log: (key, label, payload) ->
    return unless label and payload
    payload.key = key if key
    console.log label, payload # XXX
    payload = msgpack.pack payload
    @db.publish label, payload

  perform: (msg) ->
    [action, args...] = msg.toString().split '|'
    @handle[action].apply this, args

  run: (@callback) ->
    @connect =>
      @repeat_task 'orphan', 10, => @check_for_orphans()
      @heartbeat()
      @listen()

  heartbeat: ->
    @db.setex "heartbeat:#{@id}", 10, 1
    setTimeout (=> @heartbeat()), 5000

  input: (prefix, params...) ->
    opts = _.opts params
    @params[prefix] = params
    @as[prefix] = opts.as
    @is_input[prefix] = true
    Workspace.prototype[prefix] = (args...) -> @get prefix, args...

  worker: (prefix, params..., runner) ->
    @params[prefix] = params if params.length
    @runners[prefix] = runner
    Workspace.prototype[prefix] = (args...) -> @get prefix, args...

  mixin: (mixins) ->
    Workspace.mixin mixins

  on_finish: (callback) ->
    Worker.finish_callback = callback
    this

  on_clear: (callback) ->
    Worker.clear_callback = callback
    this

  wait: (deps, key) ->
    @triggers[key] = deps.length
    for dep in deps
      (@listeners[dep] ||= []).push key

  claim: (key, callback) ->
    @db.multi()
      .smembers('sources:'+key)
      .del('sources:'+key)
      .set('lock:'+key, @id)
      .sadd('active:'+@id, key)
      .srem('pending', key)
      .exec (err, arr) =>
        return @error err if err
        callback arr[0]

  job: (queue, key) ->
    if @is_dirty || @quitting
      @db.lpush queue, key
      return
    @claim key, (sources) =>
      try
        @workers[key] = new Worker key, queue, sources, this
        @workers[key].run()
      catch e
        @error e
      @pop_next()

  resume: (key, err, value) ->
    return unless worker = @workers[key]
    worker.resume(err, value)

  release: (key) ->
    console.log "Releasing #{key}" # XXX
    if worker = @workers[key]
      delete @workers[key]
      worker.implode()
    @db.multi()
      .srem('active:'+@id, key)
      .del('lock:'+key)
      .exec (err) =>
        @error err if err

  dirty: (key) ->
    @is_dirty = true
    @log key, 'redeye:dirty', {}
    if worker = @workers[key]
      console.log "Set #{key} as dirty" # XXX
      worker.dirty = true
      worker.resume()
      # TODO: i am not 100% sure this worker
      # won't just live forever... eventually
      # it *should* call @manager.release...
    @db.multi()
      .smembers('targets:'+key)
      .del(key, 'lock:'+key, 'sources:'+key, 'targets:'+key)
      .exec (err, arr) =>
        return @error err if err
        keys = arr[0].split ','
        unless keys.length and arr[0].length
          return @after_dirty()
        @db.rpush 'dirty', keys..., (err) =>
          return @error err if err
          @after_dirty()

  after_dirty: ->
    clearTimeout @dirty_timeout
    @dirty_timeout = setTimeout (=> @clean()), 1000
    @pop_next()

  clean: ->
    @is_dirty = false
    fun() while fun = @pending.pop()

  postpone: (fun) ->
    @pending.push fun

  handle:

    quit: ->
      @quitting = true
      @setTimeout (=> @terminate()), 1000

    ready: (key) ->
      return unless keys = @listeners[key]
      delete @listeners[key]
      for key in keys
        unless --@triggers[key]
          delete @triggers[key]
          @resume key

  terminate: ->
    @db.del "heartbeat:#{@id}", =>
      # TODO: drain the pool
      @clear_tasks()
      @callback?()

  finish: (key) ->
    throw 'FINISH LOCK ERROR' if @is_dirty
    @log key, 'redeye:finish', {}
    delete @workers[key]
    @db.multi()
      .set('lock:'+key, 'ready')
      .srem('active:'+@id, key)
      .exec (err) =>
        return @error err if err
        @db.publish 'control', 'ready|'+key

  repeat_task: (name, period, callback) ->
    interval = setInterval (=> @lock_task name, period, callback), period*1100
    @task_intervals.push interval

  lock_task: (name, ttl, callback) ->
    @db.setnx 'task:'+name, 1, (err, set) =>
      return @error err if err
      return unless set
      @db.expire 'task:'+name, ttl
      callback()

  check_for_orphans: ->
    @db.evalsha @scripts.orphans, 0, @queues[0], (err, num) =>
      return @error err if err
      console.log "#{@id} rescued #{num} orphans from a burning building!" if num

  clear_tasks: ->
    for interval in @task_intervals
      clearInterval interval

  # Mark that a fatal exception occurred
  error: (err) ->
    return unless err
    message = err.stack ? err
    console.log "work_queue caught error:", message
    @db.set 'fatal', message


module.exports = Manager
