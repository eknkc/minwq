local queue = KEYS[1]

if not queue then
   return redis.error_reply("Queue not specified.")
end

local delaycheck = redis.call("EXISTS", queue .. ":delaycheck")

if delaycheck == 0 then
  redis.call("SETEX", queue .. ":delaycheck", 10, 1)
  local items = redis.call('LRANGE', queue .. ":delayed", 0, -1)

  for i=1, #items do
    local item = items[i]

    if redis.call("EXISTS", item .. ":delayed") == 0 then
      redis.call("LREM", queue .. ":delayed", 0, item)
      redis.call("LPUSH", queue .. ":waiting", item)
    end
  end
end

local key = redis.call("RPOP", queue .. ":waiting")

if not key then
  return nil
end

local ttr = tonumber(redis.call("HGET", key, "ttr"))
local uniquehash = redis.call("HGET", key, "uniquehash")
local data = redis.call("HGET", key, "data")

if ttr and ttr > 0 then
  redis.call("LPUSH", queue .. ":delayed", key)
  redis.call("SETEX", key .. ":delayed", ttr, 1)
else
  redis.call("DEL", key)

  if uniquehash and uniquehash ~= "" then
    redis.call("SREM", queue .. ":unique", uniquehash)
  end
end

return { key, data, ttr }
