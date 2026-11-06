local value = redis.call('GET', KEYS[1])
if value then return {1, value} end
if redis.call('ZSCORE', KEYS[2], KEYS[1]) then
    return redis.error_reply('string_sync: dirty data missing: ' .. KEYS[1])
end
if redis.call('SET', KEYS[3], ARGV[1], 'NX', 'PX', ARGV[2]) then
    return {2}
end
return {0}
