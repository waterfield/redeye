-- require.lua
--
--   Called by redeye when a target key requires source keys
--   as dependencies. There are four possible outcomes for each:
--
--    * the source already exists:
--        Mark the dependency and return the key value
--    * the source is locked:
--        Just mark the dependency
--    * the source is not locked:
--        Mark the dependency and enqueue the key
--    * this request would cause a cycle:
--        Just return a cycle message
--
-- Inputs
--
--   queue: which work queue to put the source key on
--   target: key which is making request for dependency
--   sources: dependency keys which are being requested
--
-- Outputs
--
--   ['ok', value1, value2, ...]
--   ['cycle', key1, key2, ...]
--
-- What is going on with this code
--
--   Ok so. We're going to do a depth-first search, starting with
--   the set of source keys. We're going to look for cycles by seeing
--   if we hit the target key. *But* we're going to stop searching on
--   any keys that have a value (because they are already done, so
--   they can't have caused a cycle) or keys without a lock (because
--   we don't know what their sources are yet). We keep a map of keys
--   that have been visited so we don't repeat them. Meanwhile, record
--   the locks and values of the actual source keys. If we come through
--   without any cycles, we'll need those. The values are sent back
--   in the response. Any key without a lock needs to be queued as a
--   job so we do that too.

local queue  = ARGV[1]
local target = ARGV[2]
local values = {'ok'}
local locks = {}

-- :'(
if target == 'null' then
  target = nil
end

-- Recursive search function.
--   len: length of stack
--   key: next key to search
--   stack: key search stack
--   visited: map of visited keys
--   returns: whether a cycle was detected
local function search(key, len, stack, visited)
  len = len + 1
  stack[len] = key
  visited[key] = true
  local value = redis.call('get', key)
  local lock = redis.call('get', 'lock:'..key)
  if lock and not value then
    local deps = redis.call('smembers', 'sources:'..key)
    for _, dep in ipairs(deps) do
      if dep == target then
        return true
      elseif not visited[dep] then
        if search(dep, len, stack, visited) then
          return true
        end
      end
    end
  end
  stack[len] = nil
  return false
end

-- Don't bother checking for cycles unless the request specified
-- a target. You'd do that if you were making a seed request or
-- something.
if target then
  -- loop over source keys
  local index = 1
  local stack = {'cycle'}
  while ARGV[index + 2] do
    local key = ARGV[index + 2]
    values[index + 1] = redis.call('get', key)
    locks[key] = redis.call('get', 'lock:'..key)
    if search(key, 1, stack, {}) then
      return stack
    end
    index = index + 1
  end
end

-- no cycles, so loop through sources again
local index = 1
while ARGV[index + 2] do

  -- grab next source and its recorded lock
  local source = ARGV[index + 2]
  local lock = locks[source]
  index = index + 1

  -- record the dependency between source and target
  if target then
    redis.call('sadd', 'sources:'..target, source)
    redis.call('sadd', 'targets:'..source, target)
  end

  -- enqueue the job if it is not locked
  if not lock then
    redis.call('set', 'lock:'..source, 'queued')
    redis.call('sadd', 'pending', source)
    redis.call('lpush', queue, source)
    locks[source] = true
  end
end

-- return the values, some of which may be nil
return values
