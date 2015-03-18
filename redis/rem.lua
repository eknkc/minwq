local set = KEYS[1]
local hash = KEYS[2]

local opt = cjson.decode(ARGV[2])

redis.call("ZREM", set, opt.id)
redis.call("HDEL", hash, opt.id)

return 1
