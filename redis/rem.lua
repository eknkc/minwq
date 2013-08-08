local queue = KEYS[1]
local data = cjson.decode(ARGV[1])

local llen = redis.call("LLEN", queue)

for i=1, llen do
  local entry = redis.call('LINDEX', queue, i - 1)
  local item = cmsgpack.unpack(entry)

  if (data["id"] and item["id"] == data["id"]) or (data["unique"] and item["unique"] == data["unique"]) then
    return redis.call('LREM', queue, 0, entry)
  end
end

return 0
