-- KEYS: bitmap, deleted marker, load placeholder, pending, working
-- ARGV: task
redis.call('SET', KEYS[1], '')
redis.call('SET', KEYS[2], '1')
redis.call('DEL', KEYS[3])
if not redis.call('ZSCORE', KEYS[5], ARGV[1]) then
    local clock = redis.call('TIME')
    local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
    redis.call('ZADD', KEYS[4], 'NX', now, ARGV[1])
end
return 1
