-- KEYS: bitmap, deleted marker, load placeholder, pending, working
-- ARGV: task, offset, bit
if redis.call('EXISTS', KEYS[1]) == 0 then
    if redis.call('ZSCORE', KEYS[4], ARGV[1]) or redis.call('ZSCORE', KEYS[5], ARGV[1]) then
        return redis.error_reply('bit_map_sync: dirty bitmap missing: ' .. ARGV[1])
    end
    return 0
end
redis.call('SETBIT', KEYS[1], ARGV[2], ARGV[3])
redis.call('PERSIST', KEYS[1])
redis.call('DEL', KEYS[2], KEYS[3])
if not redis.call('ZSCORE', KEYS[5], ARGV[1]) then
    local clock = redis.call('TIME')
    local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
    redis.call('ZADD', KEYS[4], 'NX', now, ARGV[1])
end
return 1
