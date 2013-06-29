local queue = KEYS[1]
local data = KEYS[2]
local ttr = KEYS[3]

if not queue then
   return redis.error_reply("Queue not specified.")
end

if not data then
   return redis.error_reply("Data not specified.")
end

if not ttr then
   ttr = 0
else
   ttr = tonumber(ttr)
end

local id = redis.call("INCR", queue .. ":id")
local key = queue .. ":job:" .. id

redis.call("HMSET", key, "ttr", ttr, "data", data)
redis.call("LPUSH", queue .. ":waiting", key)

return key
