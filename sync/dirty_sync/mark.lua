-- KEYS: pending, working, owners, redirty
-- ARGV: JSON-encoded keys...
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
for _, task in ipairs(ARGV) do
    if redis.call('ZSCORE', KEYS[2], task) then
        redis.call('SADD', KEYS[4], task)
    else
        redis.call('ZADD', KEYS[1], 'NX', now, task)
    end
end
return 1
