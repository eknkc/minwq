local queue = KEYS[1]
local key = KEYS[2]

if not key then
   return redis.error_reply("Key not specified.")
end

local uniquehash = redis.call("HGET", key, "uniquehash")

if uniquehash and uniquehash ~= "" then
  redis.call("SREM", queue .. ":unique", uniquehash)
end

redis.call("LREM", queue .. ":running", 0, key)
redis.call("LREM", queue .. ":waiting", 0, key)
return redis.call("DEL", key)
