-- finish.lua
--
--   Called by a worker when it completes. This script is
--   used to make sure the worker still owns the key at the
--   time its result is set; if the lock was marked dirty,
--   the script will not set the value.
--
-- Inputs
--
--   channel: control channel name
--   worker_id: unique id of the worker
--   manager_id: unique id of the manager
--   key: key we're setting
--   value: value to set as the key

local channel = ARGV[1]
local wid = ARGV[2]
local mid = ARGV[3]
local key = ARGV[4]
local value = ARGV[5]

local lock = redis.call('get', 'lock:'..key)

-- if the worker still owns the lock, set the value,
-- change the lock to 'ready', and return 1
if lock == wid then
  redis.call('set', 'lock:'..key, 'ready')
  redis.call('set', key, value)
  redis.call('srem', 'active:'..mid, key)
  redis.call('publish', channel, 'ready|'..key)
  return 1
-- else just return 0 to indicate failure
else
  return 0
end
