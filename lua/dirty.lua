-- dirty.lua
--
--   One or more keys have become dirty. Delete them and their
--   recursive targets. If any key is dirty while running, mark
--   it as dirty and send a message so that its Manager can prevent
--   it from doing useless work.
--
-- Inputs
--
--   control channel
--   list of keys which are dirty

local channel = ARGV[1]
local stack = ARGV
local len = #stack
local visited = {}
local target
local count = 0

-- visit nodes until there are none left
while len > 1 do

  -- pop the last node, mark as visited
  local key = stack[len]
  len = len - 1
  visited[key] = true
  count = count + 1

  -- get the lock and targets of the key
  local lock = redis.call('get', 'lock:'..key)

  -- if the key is already done, delete its value
  if lock == 'ready' then
    redis.call('del', key)
  -- if it's being worked on, send a message
  elseif lock and (lock ~= 'queued') then
    redis.call('publish', channel, 'dirty|'..key)
  end

  if lock then
    local targets = redis.call('smembers', 'targets:'..key)

    -- delete the key's lock, sources and targets
    redis.call('del', 'lock:'..key)
    redis.call('del', 'sources:'..key)
    redis.call('del', 'targets:'..key)

    -- publish a redeye message
    local msg = cmsgpack.pack( {key=key} )
    redis.call('publish', 'redeye:dirty', msg)

    -- add each unvisisted target to node stack
    for _, target in ipairs(targets) do
      if not visited[target] then
        len = len + 1
        stack[len] = target
      end
    end
  end
end

-- return total number of keys marked dirty
return count
