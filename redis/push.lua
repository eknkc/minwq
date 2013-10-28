local set = KEYS[1]
local hash = KEYS[2]

local job = cjson.decode(ARGV[1])
local meta = cjson.decode(ARGV[2])

local score = meta.date

if job.delay then
  score = -1 * (meta.date + job.delay)
end

redis.call("ZADD", set, score, job.id)
redis.call("HSET", hash, job.id, job.data)

return job.id
