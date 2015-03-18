local set = KEYS[1]
local hash = KEYS[2]
local date = ARGV[1]

local opt = cjson.decode(ARGV[2])

local recycled = redis.call("ZRANGEBYSCORE", set, -1 * date, "(0")

for i, key in ipairs(recycled) do
  redis.call("ZADD", set, date, key)
end

local i, key = next(redis.call("ZRANGEBYSCORE", set, "(0", "+inf", "LIMIT", "0", "1"))

if key == nil then
  return nil
end

local data = redis.call("HGET", hash, key)

if opt.ttl then
  local score = -1 * (date + opt.ttl)
  redis.call("ZADD", set, score, key)
else
  redis.call("ZREM", set, key)
  redis.call("HDEL", hash, key)
end

return { key, data }
