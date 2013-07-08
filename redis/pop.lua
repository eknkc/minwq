local queue = KEYS[1]
local meta = cjson.decode(ARGV[1])

local llen = redis.call("LLEN", queue)

local i = 1
while i <= llen do
  local data = redis.call("LPOP", queue)
  local item = cmsgpack.unpack(data)

  if item["state"] == "waiting" then
    if item["ttr"] then
      item["state"] = "delayed"
      item["delayttl"] = meta["date"] + item["ttr"]

      redis.call("RPUSH", queue, cmsgpack.pack(item))
    else
      item["state"] = "active"
    end

    return cjson.encode(item)
  elseif item["state"] == "delayed" then
    if item["delayttl"] < meta["date"] then
      item["state"] = "waiting"
      redis.call("LPUSH", queue, cmsgpack.pack(item))
      i = i - 1
    else
      redis.call("RPUSH", queue, cmsgpack.pack(item))
    end
  end

  i = i + 1
end

return nil
