-- KEYS: working, owners
-- ARGV: token, lease TTL, tasks...
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
for i = 3, #ARGV do
    local deadline = redis.call('ZSCORE', KEYS[1], ARGV[i])
    if redis.call('HGET', KEYS[2], ARGV[i]) ~= ARGV[1] or not deadline or tonumber(deadline) <= now then
        return 0
    end
end
for i = 3, #ARGV do
    redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[i])
end
return 1
