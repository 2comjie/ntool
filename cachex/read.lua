local value = redis.call('GET', KEYS[1])
if value then
    return {1, value, redis.call('PTTL', KEYS[1])}
end
if redis.call('SET', KEYS[2], ARGV[1], 'NX', 'PX', ARGV[2]) then
    return {2}
end
if redis.call('GET', KEYS[2]) == ARGV[1] then
    return {2}
end
return {0}
