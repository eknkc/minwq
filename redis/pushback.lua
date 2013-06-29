local queue = KEYS[1]
local key = KEYS[2]

if not key then
  return redis.error_reply("Key not specified.")
end

if redis.call("EXISTS", key) == 0 then
  return redis.error_reply("Job not found.")
end

redis.call("LREM", queue .. ":running", 0, key)
redis.call("LPUSH", queue .. ":waiting", key)

return key
