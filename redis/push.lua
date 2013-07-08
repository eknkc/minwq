local queue = KEYS[1]
local data = cjson.decode(ARGV[1])
local meta = cjson.decode(ARGV[2])

if data["unique"] then
  local items = redis.call('LRANGE', queue, 0, -1)

  for i=1, #items do
    local item = cmsgpack.unpack(items[i])
    if item["unique"] == data["unique"] then
      return nil
    end
  end
end

if data["delay"] then
  data["delayttl"] = data["delay"] + meta["date"]
  data["state"] = "delayed"
else
  data["state"] = "waiting"
end

if data["priority"] and data["priority"] > 0 then
  redis.call("LPUSH", queue, cmsgpack.pack(data))
else
  redis.call("RPUSH", queue, cmsgpack.pack(data))
end

return data.id
