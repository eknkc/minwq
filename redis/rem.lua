local queue = KEYS[1]
local data = cjson.decode(ARGV[1])

local llen = redis.call("LLEN", queue)

for i=1, llen do
  local data = redis.call('LINDEX', queue, i - 1)
  local item = cmsgpack.unpack(data)

  if item["id"] == data["id"] then
    return redis.call('LREM', queue, data)
  end
end

return 0
