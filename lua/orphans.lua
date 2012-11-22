-- orphans.lua
--
--   Detect keys that are part of a working set whose
--   manager has died (no longer has a heartbeat). Clear
--   the locks on those keys and re-enqueue them.
--
-- Inputs
--
--   queue: where to re-enqueue orphaned keys

local queue = ARGV[1]
local actives = redis.call('keys', 'active:*')
local hearts = {}
local keys = {}
local len = 1
local key
local index
local str

-- convert 'active:*' to 'heartbeat:*'
for index, str in ipairs(actives) do
  hearts[index] = 'heartbeat:'..string.sub(str, 7)
end

-- find orphaned keys
local beats = redis.call('mget', unpack(hearts))
for index, str in ipairs(actives) do
  if not beats[index] then
    for _, key in ipairs(redis.call('smembers', str)) do
      keys[len] = key
      len = len + 1
    end
    redis.call('del', str)
  end
end

-- re-enque all of them
for _, key in ipairs(keys) do
  redis.call('sadd', 'pending', key)
  redis.call('lpush', queue, key)
end

-- return number of orphaned keys
return len
