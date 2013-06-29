local queue = KEYS[1]
local key = KEYS[2]

if not key then
   return redis.error_reply("Key not specified.")
end

redis.call("LREM", queue .. ":running", 0, key)
redis.call("LREM", queue .. ":waiting", 0, key)
return redis.call("DEL", key)
