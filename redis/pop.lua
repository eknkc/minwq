local queue = KEYS[1]

if not queue then
   return redis.error_reply("Queue not specified.")
end

local gcsurpress = redis.call("EXISTS", queue .. ":gcsurpress")

if gcsurpress == 0 then
  redis.call("SETEX", queue .. ":gcsurpress", 10, 1)
  local items = redis.call('LRANGE', queue .. ":running", 0, -1)

  for i=1, #items do
    local item = items[i]

    if redis.call("EXISTS", item .. ":running") == 0 then
      redis.call("LREM", queue .. ":running", 0, item)
      redis.call("LPUSH", queue .. ":waiting", item)
    end
  end
end

local key = redis.call("RPOP", queue .. ":waiting")

if not key then
  return nil
end

local ttr = tonumber(redis.call("HGET", key, "ttr"))
local data = redis.call("HGET", key, "data")

if ttr > 0 then
  redis.call("LPUSH", queue .. ":running", key)
  redis.call("SETEX", key .. ":running", ttr, 1)
else
  redis.call("DEL", key)
end

return { key, data, ttr }
