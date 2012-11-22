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
    @pop.blpop @queues..., 0, (err, response) =>
      return @error err if err
      [type, value] = response
      @job type, value

  unrequire: (source, target) ->
    @log null, 'redeye:unrequire', { source, target }

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

  wait: (deps, key) ->
    @triggers[key] = deps.length
    for dep in deps
      (@listeners[dep] ||= []).push key

  claim: (key, callback) ->
    id = uuid.v1()
    @db.multi()
      .smembers('sources:'+key)
      .del('sources:'+key)
      .set('lock:'+key, id)
      .sadd('active:'+@id, key)
      .srem('pending', key)
      .exec (err, arr) =>
        return @error err if err
        deps = if arr[0].length then arr[0].split(',') else []
        callback id, deps

  job: (queue, key) ->
    @claim key, (id, sources) =>
      try
        @workers[key] = new Worker id, key, queue, sources, this
        @workers[key].run()
      catch e
        @error e
      @pop_next()

  resume: (key, dirty) ->
    return unless worker = @workers[key]
    worker.dirty = true if dirty
    worker.resume()

  handle:

    quit: ->
      @quitting = true
      @setTimeout (=> @terminate()), 1000

    dirty: (key) ->
      if worker = @workers[key]
        worker.dirty = true
        @db.srem 'active:'+@id, key
        delete @workers[key]
      @handle.ready.apply @, [key, true]

    ready: (key, dirty) ->
      return unless keys = @listeners[key]
      delete @listeners[key]
      for key in keys
        unless --@triggers[key]
          delete @triggers[key]
          @resume key, dirty

  terminate: ->
    @db.del "heartbeat:#{@id}", =>
      # TODO: drain the pool
      @clear_tasks()
      @callback?()

  finish: (id, key, value, callback) ->
    @db.evalsha @scripts.finish, 0, id, @id, key, value, (err, set) =>
      @log(key, 'redeye:finish', {}) if set
      callback set

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
      console.log "#{@id} rescued #{num} orphans" if num

  clear_tasks: ->
    for interval in @task_intervals
      clearInterval interval

  # Mark that a fatal exception occurred
  error: (err) ->
    return unless err
    message = err.stack ? err
    console.log "Manager caught error:", message
    @db.set 'fatal', message


module.exports = Manager
