require 'fibers'
fs = require 'fs'
redis = require 'redis'
coffee = require 'coffee-script'
msgpack = require 'msgpack'
Manager = require './manager'
_ = require './util'

port = 6379
host = '127.0.0.1'
verbose = false

$ = {}
tests = []
test_name = []
context = {workers: [], lets: {}, expects: []}
stack = []

db = null
sub = null
fiber = null
requested = null
manager = null
the_test = null
passed = 0
failed = 0

debug = (args...) ->
  if verbose
    console.log args...

describe = (name, body) ->
  clone_context()
  test_name.push name
  body()
  context = stack.pop()
  test_name.pop()

clone_context = ->
  stack.push context
  context = deep_clone context

deep_clone = (obj) ->
  if _.isArray(obj)
    _.map obj, deep_clone
  else if typeof(obj) == 'object'
    clone = {}
    for own k, v of obj
      clone[k] = deep_clone v
    clone
  else
    obj

setup = (body) ->
  context.setup = body

worker = (args...) ->
  context.workers.push args

want = (value) ->
  context.expects.push -> wanted value

expect = (body) ->
  context.expects.push body

has = (hash) ->
  for key, value of hash
    context.lets[key] = value

test = (name, body) ->
  describe name, ->
    body()
    context.name = test_name.join(' ')
    tests.push context

fail = (msg) ->
  msg = msg.stack || msg
  console.log "#{the_test.name}: failed: #{msg}"
  the_test.failed = true
  failed++
  false

pass = (msg) ->
  debug "#{the_test.name}: passed"
  passed++
  true

wait = (time) ->
  setTimeout (->
    debug 'run from wait'
    fiber.run()
  ), time
  debug 'yield from wait'
  yield()

get = (args...) ->
  key = new Buffer(args.join ':')
  db.get key, (err, buf) ->
    obj = msgpack.unpack(buf) if buf
    debug 'run from get'
    fiber.run [err, obj]
  debug 'yield from get'
  [err, obj] = yield()
  throw err if err
  obj

set = (args..., value) ->
  key = args.join ':'
  buf = msgpack.pack value
  db.set key, value, (err) ->
    debug 'run from set'
    fiber.run err
  debug 'yield from set'
  err = yield()
  throw err if err
  null

diff_message = (expected, actual, prefix='    ') ->
  if _.isArray expected
    array_diff_message expected, actual, prefix
  else if _.isObject expected
    object_diff_message expected, actual, prefix
  else
    value_diff_message expected, actual, prefix

value_diff_message = (expected, actual, prefix='') ->
  "#{prefix}- #{expected}\n#{prefix}+ #{actual}\n"

array_diff_message = (expected, actual, prefix='    ') ->
  len = Math.max(expected.length, actual.length)
  msg = ''
  for i in [0...len]
    a = expected[i]
    b = actual[i]
    if a and b
      unless _.isEqual(a,b)
        msg += "#{prefix}[#{i}]\n" + diff_message(a,b,prefix+'  ')
    else if a
      msg += "#{prefix}[#{i}] - #{JSON.stringify(a)}\n"
    else if b
      msg += "#{prefix}[#{i}] + #{JSON.stringify(b)}\n"
  msg

object_diff_message = (expected, actual, prefix='    ') ->
  msg = ''
  for k, a of expected
    if (b = actual[k])?
      if !_.isEqual(a, b)
        msg += "#{prefix}[#{k}]\n#{diff_message a, b, prefix+'  '}"
    else
      msg += "#{prefix}[#{k}]\n#{prefix}  - #{JSON.stringify(a)}\n"
  for k, b of actual
    unless k of expected
      msg += "#{prefix}[#{k}]\n#{prefix}  + #{JSON.stringify(b)}\n"
  msg

error_message = (key, parts) ->
  list = ["Key \"#{key}\" encountered an error"]
  for item in parts
    { key, trace } = item
    list.push "    In worker: #{key}"
    for line in trace.split "\n"
      list.push "        #{line}"
  list.join("\n") + "\n"

missing_message = (key) ->
  "Key \"#{key}\" resulted in null\n"

delete_nodes = (val) ->
  if _.isArray val
    for item, i in val
      delete item.node_id if item
  else if _.isObject val
    delete val.node_id
  val

assert =
  equal: (a, b, msg='mismatch') ->
    if is_equal a, b
      pass()
    else
      fail(msg + "\n" + diff_message(a, b))

compare = (key, actual, expected) ->
  if !actual?
    fail missing_message(key)
  else if actual.error
    fail error_message(key, actual.error)
  else
    actual = delete_nodes actual
    if is_equal actual, expected
      pass()
    else
      msg = "Key \"#{key}\" was wrong:\n"
      msg = msg + diff_message(expected, actual)
      fail msg

is_equal = (a, b) ->
  return true if !a && !b
  return false if !a? || !b?
  if _.isObject a
    return false unless _.isObject b
    for k of a
      return false unless k of b
    for k of b
      return false unless k of a
    for k of a
      return false unless is_equal a[k], b[k]
  else if _.isArray a
    return false unless _.isArray b
    return false unless a.length == b.length
    for i, a_ in a
      return false unless is_equal a_, b[i]
  else if typeof(a) == 'number'
    return false unless typeof(b) == 'number'
    return Math.abs(a-b) < 0.0001
  else
    return _.isEqual a, b
  true

start_tests = ->
  db = redis.createClient port, host, detect_buffers: true
  sub = redis.createClient port, host, return_buffers: true
  sub.subscribe 'redeye:finish'
  sub.on 'message', (ch, msg) ->
    msg = msgpack.unpack msg
    if msg.key == requested
      manager.quit()
  fiber = Fiber ->
    run_next_test()
  debug 'run from start'
  fiber.run()

run_next_test = ->
  if tests.length
    the_test = tests.shift()
    run_test()
  else
    finish_tests()

finish_tests = ->
  report()
  db.end()
  sub.end()
  fiber = null
  exit()

exit = ->
  process.exit failed

test_is_ok = ->
  if !the_test.expects.length
    fail "does not have any expectations"
  else
    true

run_test = ->
  requested = null
  if test_is_ok()
    add_lets()
    run_setup()
    run_expect()
  run_next_test()

lazy = (fun) ->
  value = undefined
  return ->
    value = fun() if typeof(value) == 'undefined'
    value

add_lets = ->
  $ = {}
  for name, value of the_test.lets
    if typeof(value) == 'function'
      value = lazy value
    $[name] = value

add_workers = ->
  for args in the_test.workers
    manager.worker args...

run_setup = ->
  manager = new Manager { verbose, flush: true }
  add_workers()
  manager.run ->
    debug 'run from run_setup'
    fiber.run()
  wait 100
  try
    the_test.setup()
  catch err
    fail err
    manager.quit()
  debug 'yield from run_setup'
  yield()

wanted = (expected) ->
  compare requested, get(requested), expected

run_expect = ->
  return if the_test.failed
  try
    a_test() for a_test in the_test.expects
  catch err
    fail err

report = ->
  console.log "#{passed} passed,",
              "#{failed} failed",
              "(#{passed + failed} total)"

request = (args...) ->
  key = args.join ':'
  requested = key
  manager.request key, (err) ->
    debug 'run from request'
    fiber.run err
  debug 'yield from request'
  err = yield()
  throw err if err
  null

load_tests = ->
  for i in [2...process.argv.length]
    file = process.argv[i]
    raw = fs.readFileSync(file).toString()
    eval coffee.compile(raw)

load_tests()
start_tests()
