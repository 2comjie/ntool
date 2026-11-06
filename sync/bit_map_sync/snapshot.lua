-- KEYS: (bitmap, deleted marker)...
local result = {}
for i = 1, #KEYS, 2 do
    local value = redis.call('GET', KEYS[i])
    if not value then
        return redis.error_reply('bit_map_sync: dirty bitmap missing: ' .. KEYS[i])
    end
    result[#result + 1] = value
    result[#result + 1] = redis.call('EXISTS', KEYS[i + 1])
end
return result
