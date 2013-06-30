local queue = KEYS[1]

local data = ARGV[1]
local uniquehash = ARGV[2]
local priority = tonumber(ARGV[3])
local delay = tonumber(ARGV[4])
local ttr = tonumber(ARGV[5])

if not queue then
   return redis.error_reply("Queue not specified.")
end

if not data then
   return redis.error_reply("Data not specified.")
end

local id = redis.call("INCR", queue .. ":id")
local key = queue .. ":job:" .. id

if uniquehash ~= "" then
  if redis.call("SADD", queue .. ":unique", uniquehash) == 0 then
    return nil
  else
    redis.call("HSET", key, "uniquehash", uniquehash)
  end
end

redis.call("HSET", key, "data", data)

if ttr > 0 then
  redis.call("HSET", key, "ttr", ttr)
end

if delay > 0 then
  redis.call("LPUSH", queue .. ":delayed", key)
  redis.call("SETEX", key .. ":delayed", delay, 1)
else
  if priority > 0 then
    redis.call("RPUSH", queue .. ":waiting", key)
  else
    redis.call("LPUSH", queue .. ":waiting", key)
  end
end

return key
