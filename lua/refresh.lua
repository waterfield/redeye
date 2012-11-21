-- seed is the initial key to re-expire
-- ttl is the new TTL value for seed and its recursive dependencies
local seed = ARGV[1]
local ttl = ARGV[2]

-- visited keeps track of which nodes we hit
-- stack has a list of nodes to visit next
local visited = {}
local stack = {seed}
local len = 1
local count = 0

-- visit nodes until none are left
while len > 0 do
  -- take key from stack, mark as visited
  local key = stack[len]
  stack[len] = nil
  len = len - 1
  count = count + 1
  -- expire the key, its lock, sources, and targets
  redis.pcall('expire', key, ttl)
  redis.pcall('expire', 'lock:'..key, ttl)
  redis.pcall('expire', 'sources:'..key, ttl)
  redis.pcall('expire', 'targets:'..key, ttl)
  -- for each source of that key
  local sources = redis.call('smembers', 'sources:'..key)
  for _, source in ipairs(sources) do
    -- visit that source if we haven't already
    if not visisted[source] then
      len = len + 1
      stack[len] = source
    end
  end
end

-- return how many keys were re-expired
return count
