local wqPrefix = KEYS[1]
local prefix = KEYS[2]
local queue = KEYS[3]

prefix = wqPrefix .. ":" .. prefix
if queue then
   prefix = prefix .. ":" .. queue
end
prefix = prefix .. "*"

local items = redis.call("KEYS", prefix)

for i=1, #items do
  local item = items[i]
  redis.call("DEL", item)
end
