-- KEYS: pending, working, data hash, dirty fields set, (load, miss)...
-- ARGV: (task, field, payload)...
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
for i = 1, #ARGV, 3 do
    local index = (i - 1) / 3
    redis.call('HSET', KEYS[3], ARGV[i + 1], ARGV[i + 2])
    redis.call('SADD', KEYS[4], ARGV[i + 1])
    if not redis.call('ZSCORE', KEYS[2], ARGV[i]) then
        redis.call('ZADD', KEYS[1], 'NX', now, ARGV[i])
    end
    redis.call('DEL', KEYS[5 + index * 2], KEYS[6 + index * 2])
end
redis.call('PERSIST', KEYS[3])
return 1
