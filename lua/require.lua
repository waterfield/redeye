-- require.lua
--
--   Called by redeye when a target key requires a source key
--   as a dependency. There are four possible outcomes:
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
--   source: dependency key which is being requested
--   target: key which is making request for dependency
local queue  = ARGV[1]
local source = ARGV[2]
local target = ARGV[3]

-- get the value of the source key
local value = redis.call('get', source)

-- skip cycle detection if the value is alrady complete
-- or if there is no defined target
if target and not value then

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
    local sources = redis.call('smembers', 'sources:'..key)
    for _, source in ipairs(sources) do
      -- if we loop back to the requester, indicate a cycle
      if source == target then
        return {'cycle'}
      -- otherwise, visit the source
      elseif not visited[source] then
        len = len + 1
        stack[len] = source
      end
    end
  end
end

-- record the dependency between source and target
if target then
  redis.call('sadd', 'sources:'..target, source)
  redis.call('sadd', 'targets:'..source, target)
end

-- if the value is already complete, return it
if value then
  return {'exists', value}
-- if the key is already being worked on, return
elseif 0 == redis.call('setnx', 'lock:'..source, 'queued') then
  return {'locked'}
-- otherwise, enqueue the key as a job
else
  redis.call('sadd', 'pending', source)
  redis.call('lpush', queue, source)
  return 'pushed'
end
