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
local len = 0
local key
local index
local str

-- convert 'active:*' to 'heartbeat:*'
for index, str in ipairs(actives) do
  hearts[index] = 'heartbeat:'..string.sub(str, 8)
end

-- find orphaned keys
if hearts[1] then
  local beats = redis.call('mget', unpack(hearts))
  for index, str in ipairs(actives) do
    if not beats[index] then
      for _, key in ipairs(redis.call('smembers', str)) do
        len = len + 1
        keys[len] = key
      end
      redis.call('del', str)
    end
  end
end

-- re-enque all of them
for _, key in ipairs(keys) do
  redis.call('sadd', 'pending', key)
  redis.call('lpush', queue, key)
  local msg = cmsgpack.pack({key=key})
  redis.call('publish', 'redeye:orphan', msg)
end

-- -- if the queue runs dry, safely re-enqueue pending keys
-- local queue_len = redis.call('llen', queue)
-- if queue_len == 0 then
--   local dropped = redis.call('sinter', 'pending', 'lost')
--   redis.call('sdiffstore', 'lost', 'pending', 'lost')
--   for _, key in ipairs(dropped) do
--     redis.call('lpush', queue, key)
--     local msg = cmsgpack.pack({key=key})
--     redis.call('publish', 'redeye:orphan', msg)
--   end
-- else
--   redis.call('del', 'lost')
-- end

-- return number of orphaned keys
return len
