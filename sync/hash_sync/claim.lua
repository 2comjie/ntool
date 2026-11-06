-- KEYS: pending, working (lease deadlines), owners
-- ARGV: token, lease TTL, batch size
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, ARGV[3])
for _, task in ipairs(expired) do
    redis.call('ZREM', KEYS[2], task)
    redis.call('HDEL', KEYS[3], task)
    redis.call('ZADD', KEYS[1], 'NX', now, task)
end
local tasks = redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[3]) - 1)
for _, task in ipairs(tasks) do
    redis.call('ZREM', KEYS[1], task)
    redis.call('ZADD', KEYS[2], now + tonumber(ARGV[2]), task)
    redis.call('HSET', KEYS[3], task, ARGV[1])
end
return tasks
