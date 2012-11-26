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
--   ['cycle', source1, source2, ...]
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
local cycles = {'cycle'}
local locks = {}
local ncycles = 0

-- :(
if target == 'null' then
  target = nil
end

-- Don't bother checking for cycles unless the request specified
-- a target. You'd do that if you were making a seed request or
-- something.
if target then
  -- loop over source keys
  local index = 1
  while ARGV[index + 2] do
    -- initial stack consists of just the source key
    local source = ARGV[index + 2]
    local stack = {source}
    local len = 1
    local visited = {}
    local first = true
    -- loop until all keys are visited
    while len > 0 do
      -- grab the key's value and lock
      local key = stack[len]
      len = len - 1
      local value = redis.call('get', key)
      local lock = redis.call('get', 'lock:'..key)
      -- if this is a source key, record the lock and value
      if first then
        locks[index - 1] = lock
        values[index - 2] = value
        first = false
      end
      -- mark the key visited so we don't repeat it
      visited[key] = true
      -- stop iterating unless a cycle is even possible (see header)
      if lock and not value then
        -- loop over the key's dependencies
        local deps = redis.call('smembers', 'sources:'..key)
        for _, dep in ipairs(deps) do
          -- if the dependency is the target, it's a cycle, so
          -- bump the cycle count and record the source as causing it
          if dep == target then
            ncycles = ncycles + 1
            cycles[ncycles + 1] = source
            len = 0
            break
          -- otherwise, visit the dependency unless we have already
          elseif not visited[dep] then
            len = len + 1
            stack[len] = dep
          end
        end
      end
    end
    -- move on to the next source
    index = index + 1
  end
end

-- if we had any cycles, return them with the 'cycle' status
if ncycles > 0 then
  return cycles
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
