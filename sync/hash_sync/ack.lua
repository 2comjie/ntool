-- KEYS: pending, working, owners, (data hash, dirty fields set, miss key)...
-- ARGV: token, miss TTL, (task, field, snapshot, deleted, cache TTL)...
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
for i = 3, #ARGV, 5 do
    local deadline = redis.call('ZSCORE', KEYS[2], ARGV[i])
    if redis.call('HGET', KEYS[3], ARGV[i]) ~= ARGV[1] or not deadline or tonumber(deadline) <= now then
        return 0
    end
    local index = (i - 3) / 5
    if not redis.call('HGET', KEYS[4 + index * 3], ARGV[i + 1]) then
        return redis.error_reply('hash_sync: dirty field missing: ' .. ARGV[i])
    end
end
for i = 3, #ARGV, 5 do
    local index = (i - 3) / 5
    local data = KEYS[4 + index * 3]
    local dirty = KEYS[5 + index * 3]
    if redis.call('HGET', data, ARGV[i + 1]) == ARGV[i + 2] then
        redis.call('SREM', dirty, ARGV[i + 1])
        if ARGV[i + 3] == '1' then
            redis.call('HDEL', data, ARGV[i + 1])
            redis.call('SET', KEYS[6 + index * 3], '1', 'PX', ARGV[2])
        end
        if redis.call('SCARD', dirty) == 0 then
            redis.call('PEXPIRE', data, ARGV[i + 4])
        end
    else
        redis.call('ZADD', KEYS[1], 'NX', now, ARGV[i])
    end
    redis.call('HDEL', KEYS[3], ARGV[i])
    redis.call('ZREM', KEYS[2], ARGV[i])
end
return 1
