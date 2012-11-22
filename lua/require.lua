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
local queue  = ARGV[1]
local target = ARGV[2]
local values = {'ok'}
local cycles = {'cycle'}
local locks = {}
local ncycles = 0
local index = 1

while ARGV[index + 2] do

  -- get the value and lock for the key, store them for later
  local source = ARGV[index + 2]
  local value = redis.call('get', source)
  local lock = redis.call('get', 'lock:'..source)
  values[index + 1] = value
  locks[source] = lock
  index = index + 1

  -- skip cycle detection if the value is alrady complete
  -- or if there is no defined target
  if target and lock and not value then

    -- visited keeps track of which nodes we hit
    -- stack has a list of nodes to visit next
    local visited = {}
    local stack = {source}
    local len = 1

    -- visit nodes until none are left
    while len > 0 do
      -- take key from stack, mark as visited
      local key = stack[len]
      stack[len] = nil
      len = len - 1
      visited[key] = true
      -- for each source of that key
      local deps = redis.call('smembers', 'sources:'..key)
      for _, dep in ipairs(deps) do
        -- if we loop back to the requester, indicate a cycle
        if dep == target then
          ncycles = ncycles + 1
          cycles[ncycles + 1] = source
          len = 0
          break
        -- otherwise, visit the source
        elseif not visited[dep] then
          len = len + 1
          stack[len] = dep
        end
      end
    end
  end
end

if ncycles > 0 then
  return cycles
end

index = 1
while ARGV[index + 2] do

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

return values
