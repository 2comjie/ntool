-- KEYS: pending, working, owners, redirty
-- ARGV: token, tasks...
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
for i = 2, #ARGV do
    local deadline = redis.call('ZSCORE', KEYS[2], ARGV[i])
    if redis.call('HGET', KEYS[3], ARGV[i]) ~= ARGV[1] or not deadline or tonumber(deadline) <= now then
        return 0
    end
end
for i = 2, #ARGV do
    redis.call('HDEL', KEYS[3], ARGV[i])
    redis.call('ZREM', KEYS[2], ARGV[i])
    if redis.call('SREM', KEYS[4], ARGV[i]) == 1 then
        redis.call('ZADD', KEYS[1], 'NX', now, ARGV[i])
    end
end
return 1
