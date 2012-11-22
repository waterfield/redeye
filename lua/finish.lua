-- finish.lua
--
--   Called by a worker when it completes. This script is
--   used to make sure the worker still owns the key at the
--   time its result is set; if the lock was marked dirty,
--   the script will not set the value.
--
-- Inputs
--
--   worker_id: unique id of the worker
--   manager_id: unique id of the manager
--   key: key we're setting
--   value: value to set as the key

local wid = ARGV[1]
local mid = ARGV[2]
local key = ARGV[3]
local value = ARGV[4]

local lock = redis.call('get', 'lock:'..key)

-- if the worker still owns the lock, set the value,
-- change the lock to 'ready', and return 1
if lock == wid then
  redis.call('set', 'lock:'..key, 'ready')
  redis.call('set', key, value)
  redis.call('srem', 'active:'..mid, key)
  redis.call('publish', 'control', 'ready|'..key)
  return 1
-- else just return 0 to indicate failure
else
  return 0
end
