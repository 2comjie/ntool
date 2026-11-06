-- KEYS: pending, working, owners
-- ARGV: token, tasks...
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
for i = 2, #ARGV do
    if redis.call('HGET', KEYS[3], ARGV[i]) == ARGV[1] then
        redis.call('HDEL', KEYS[3], ARGV[i])
        redis.call('ZREM', KEYS[2], ARGV[i])
        redis.call('ZADD', KEYS[1], 'NX', now, ARGV[i])
    end
end
return 1
