# test.coffee
# ===========

# Running
# -------
#
#     coffee test.coffee -w path_to_worker_file test_file1 test_file2 ...

# The code
# --------

require 'fibers'
fs = require 'fs'
redis = require 'redis'
coffee = require 'coffee-script'
msgpack = require 'msgpack'
Manager = require './manager'
_ = require './util'

argv = require('optimist').argv

port = argv.p ? 6379
host = argv.h ? '127.0.0.1'
verbose = argv.v?

$ = {}
ext = {}
tests = []
test_name = []
context = {workers: [], lets: {}, expects: [], setups: []}
stack = []

db = null
sub = null
fiber = null
requested = null
manager = null
the_test = null
passed = 0
failed = 0

# Print a message in verbose mode only
debug = (args...) ->
  if verbose
    console.log args...

# A container for other tests, who will inherit any
# setup or expect blocks.
describe = (name, body) ->
  clone_context()
  test_name.push name
  body()
  context = stack.pop()
  test_name.pop()

# Push the current test context to a stack and clone it.
# This way each test knows the specific, inherited context
# from whence it was defined.
clone_context = ->
  stack.push context
  context = deep_clone context

# Create a deep clone of an object. Works for arrays too.
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

# Add a 'setup' block to the current test context.
# Setup blocks run in sequence before running the test
# expectations.
setup = (body) ->
  context.setups.push body

# Define a worker which will be available while the test
# runs. Has the same syntax as `Manager#worker`.
worker = (args...) ->
  context.workers.push args

# A shorthand way of saying that the key requested by the
# setup block is expected to be equal to this given value.
# This adds an expectation block, but others can be defined
# as well.
want = (value) ->
  context.expects.push -> wanted value

# Add an expectation block. They will be run after the
# setup blocks, and after the test's requested key has
# completed running.
expect = (body) ->
  context.expects.push body

# Define a $.key value for the current test context. You can
# define these in sub-tests in a `describe` block, and when
# shared `setup` or `expect` blocks (or workers) run, they will
# use the value set by the currently running test. You can use
# this to reduce duplicate code in the test.
has = (hash) ->
  for key, value of hash
    context.lets[key] = value

# Add a test. The test inherits the current test context,
# including all parent setup, expect, and workers. Pushes
# the test onto the global 'test' list.
test = (name, body) ->
  describe name, ->
    body()
    context.name = test_name.join(' ')
    tests.push context

# Call this to indicate a test has failed. The test will
# NOT abort, however; all tests are always run.
fail = (msg) ->
  msg = msg.stack || msg
  console.log "#{the_test.name}: failed: #{msg}"
  the_test.failed = true
  failed++
  false

# Call this to indicate a test has passed. Like `fail`, can
# be called multiple times per test.
pass = (msg) ->
  debug "#{the_test.name}: passed"
  passed++
  true

# Pause execution, which also pauses the fiber; returns
# after the given number of milliseconds. Other event handlers
# will still be active during this time.
wait = (time) ->
  setTimeout (->
    debug 'run from wait'
    fiber.run()
  ), time
  debug 'yield from wait'
  yield()

# Mimics functionality of Manager#pack, for packed-form keys
pack_fields = (hash, fields) ->
  hash[field] for field in fields

# Mimics functionality of Manager#unpack, for packed-form keys
unpack_fields = (array, fields) ->
  hash = {}
  for field, index in fields
    hash[field] = array[index]
  hash

# Convert an asynchronous operation into a synchronous one.
# You must use this if you want to do something asynchronous
# in a test. The async method provides a callback which should
# be called with two arguments, `err` and `value`. `err` will
# be thrown from within the fiber, if present. Otherwise, `value`
# will be the return value of `async`. Like `wait`, other event
# handlers will be active during this time.
async = (callback) ->
  callback (err, value) ->
    fiber.run [err, value]
  [err, value] = yield()
  throw err if err
  value

# Get a key from the database and return its unpacked value. Commonly
# would be called from an `expect` block.
get = (args...) ->
  key = new Buffer(args.join ':')
  prefix = key.toString().split(':')[0]
  db.get key, (err, buf) ->
    obj = msgpack.unpack(buf) if buf
    if pack = manager.pack[prefix]
      obj = unpack_fields obj, pack
    debug 'run from get'
    fiber.run [err, obj]
  debug 'yield from get'
  [err, obj] = yield()
  throw err if err
  obj

# Set a key into the database (after packing it). Commonly would
# be called from a `setup` block.
set = (args..., value) ->
  key = args.join ':'
  prefix = key.split(':')[0]
  if pack = manager.pack[prefix]
    value = pack_fields value, pack
  buf = msgpack.pack value
  db.multi()
    .set(key, buf)
    .set('lock:'+key, 'ready')
    .exec (err) =>
      debug 'run from set'
      fiber.run err
  debug 'yield from set'
  err = yield()
  throw err if err
  null

# Given two values are not identical, create a nested
# diff of the two objects, which can handle both arrays and
# objects.
#
# TODO: this doesn't work right when 'actual' and 'expected'
#       are not the same type, i.e. object vs string.
diff_message = (expected, actual, prefix='    ') ->
  if _.isArray expected
    array_diff_message expected, actual, prefix
  else if _.isObject expected
    object_diff_message expected, actual, prefix
  else
    value_diff_message expected, actual, prefix

# Simplest diff message, for a scalar value difference
value_diff_message = (expected, actual, prefix='') ->
  "#{prefix}- #{expected}\n#{prefix}+ #{actual}\n"

# Compare two arrays, indicating which indices are different, and
# with a nested diff message.
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

# Compare two objects, indicating which properties are different,
# and with a nested diff message.
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

# Format an error message for a particular key.
error_message = (key, parts) ->
  list = ["Key \"#{key}\" encountered an error"]
  for item in parts
    { key, trace } = item
    list.push "    In worker: #{key}"
    for line in trace.split "\n"
      list.push "        #{line}"
  list.join("\n") + "\n"

# Create an error message indicating a key didn't produce
# a value at all.
missing_message = (key) ->
  "Key \"#{key}\" resulted in null\n"

# Redeye keys may produce 'node' information for advanced logging;
# cull this information before doing comparisons.
delete_nodes = (val) ->
  if _.isArray val
    for item, i in val
      delete item.node_id if item
  else if _.isObject val
    delete val.node_id
  val

# Ultra-simple 'assert' module, only containing `that` and `equal`
assert =
  that: (bool, msg='nope') ->
    if bool
      pass()
    else
      fail(msg)
  equal: (a, b, msg='mismatch') ->
    if is_equal a, b
      pass()
    else
      fail(msg + "\n" + diff_message(a, b))

# Compare two things for equality. Pass or fail the test based
# on this, and print a recursive diff error. The 'key' argument
# is optional and defaults to the test's requested key.
compare = (key, actual, expected) ->
  if arguments.length == 2
    [key, actual, expected] = [requested, key, actual]
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

# Determine if two objects are recursively equal to each other.
# However, for numbers, only assert that the numbers are within
# a tolerance of 0.0001.
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

# Kick off the tests by connecting to the database, listening for
# messages, and starting the test-runner fiber.
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

# Run the next test in the sequence. If all tests have been run,
# wrap up and exit.
run_next_test = ->
  if tests.length
    the_test = tests.shift()
    run_test()
  else
    finish_tests()

# Report on what tests passed and failed, then disconnect from
# the database, kill the test-runner fiber, and exit the process
# with an appropriate error code.
finish_tests = ->
  report()
  db.end()
  sub.end()
  fiber = null
  exit()

# Exit the process. The exit code will be 0 on success, nonzero
# if any test failed (it will actually equal the number of failed tests)
exit = ->
  process.exit failed

# Check if the test even makes any sense - each test needs at least
# one expectation portion.
test_is_ok = ->
  if !the_test.expects.length
    fail "does not have any expectations"
  else
    true

# Run the current test. First apply its context ($) data, then run
# its setup blocks, then block until the manager finishes the requested
# key. Finally, run the expect blocks.
run_test = ->
  requested = null
  if test_is_ok()
    add_lets()
    run_setup()
    run_expect()
  run_next_test()

# Create a wrapper function around a `has foo: -> bar` context,
# such that the actual 'foo' function is only called once and its
# value memoized.
lazy = (fun) ->
  value = undefined
  return ->
    value = fun() if typeof(value) == 'undefined'
    value

# Add the 'has' declarations from the test to the global '$'
# context. Any declarations that are functions are wrapped in
# lazy evaluators.
add_lets = ->
  $ = {}
  for name, value of the_test.lets
    if typeof(value) == 'function'
      value = lazy value
    $[name] = value

# Add all the test's defined workers to the manager; also, if the
# -w flag was specified, require that module and initialize it on
# the manager.
add_workers = ->
  if argv.w
    require(argv.w).init manager
  for args in the_test.workers
    manager.worker args...

# Set up the test scenario. create a new manager, apply test workers
# to it, and start it running. Then run setup blocks, and wait pending
# the manager to finish the requested key. If no key was requested,
# continue immediately to the expectation blocks.
run_setup = ->
  manager = new Manager { verbose, flush: true }
  add_workers()
  manager.on 'ready', ->
    fiber.run()
  manager.run ->
    debug 'run from run_setup'
    fiber.run()
  yield()
  try
    setup() for setup in the_test.setups
  catch err
    fail err
    manager.quit()
  debug 'yield from run_setup'
  yield() if requested

# Go get a certain key and compare it to the given expected value.
# If only called with one argument, the key is assumed to be the
# requested key for the test.
wanted = (args..., expected) ->
  key = if args.length then args.join(':') else requested
  compare key, get(key), expected

# Run the expectation blocks for the test; if an error is
# raised, catch it and just fail the test with the error as
# a message.
run_expect = ->
  return if the_test.failed
  try
    a_test() for a_test in the_test.expects
  catch err
    fail err

# Report how many assertions passed and failed.
report = ->
  console.log "#{passed} passed,",
              "#{failed} failed",
              "(#{passed + failed} total)"

# Set the requested key for the test. Every test can have at
# most one. The manager will request the key on the job equeue,
# and will begin calling expectation blocks only once the key
# has been completed.
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

# Files specified to test.coffee are loaded into the test's namespace
# with `eval`.
load_tests = ->
  for file in argv._
    raw = fs.readFileSync(file).toString()
    eval coffee.compile(raw)

load_tests()
start_tests()
