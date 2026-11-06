-- KEYS: data hash, dirty fields set, load placeholder, negative cache
-- ARGV: field, token, load timeout milliseconds
local value = redis.call('HGET', KEYS[1], ARGV[1])
if value then return {1, value} end
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
    return redis.error_reply('hash_sync: dirty field missing: ' .. ARGV[1])
end
if redis.call('EXISTS', KEYS[4]) == 1 then return {1, ''} end
if redis.call('SET', KEYS[3], ARGV[2], 'NX', 'PX', ARGV[3]) then return {2} end
return {0}
