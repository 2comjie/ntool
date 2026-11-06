-- KEYS: bitmap, load placeholder, pending, working
-- ARGV: task, offset, token, load timeout milliseconds
if redis.call('EXISTS', KEYS[1]) == 1 then
    return {1, redis.call('GETBIT', KEYS[1], ARGV[2])}
end
if redis.call('ZSCORE', KEYS[3], ARGV[1]) or redis.call('ZSCORE', KEYS[4], ARGV[1]) then
    return redis.error_reply('bit_map_sync: dirty bitmap missing: ' .. ARGV[1])
end
if redis.call('SET', KEYS[2], ARGV[3], 'NX', 'PX', ARGV[4]) then return {2} end
return {0}
