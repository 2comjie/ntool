-- KEYS: pending, working, owners, (bitmap, deleted marker)...
-- ARGV: token, miss TTL, (task, raw snapshot, deleted, cache TTL)...
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
for i = 3, #ARGV, 4 do
    local deadline = redis.call('ZSCORE', KEYS[2], ARGV[i])
    if redis.call('HGET', KEYS[3], ARGV[i]) ~= ARGV[1] or not deadline or tonumber(deadline) <= now then
        return 0
    end
    local index = (i - 3) / 4
    if redis.call('EXISTS', KEYS[4 + index * 2]) == 0 then
        return redis.error_reply('bit_map_sync: dirty bitmap missing: ' .. ARGV[i])
    end
end
for i = 3, #ARGV, 4 do
    local index = (i - 3) / 4
    local data = KEYS[4 + index * 2]
    local deleted = KEYS[5 + index * 2]
    if redis.call('GET', data) == ARGV[i + 1] and redis.call('EXISTS', deleted) == tonumber(ARGV[i + 2]) then
        if ARGV[i + 2] == '1' then
            redis.call('SET', data, '', 'PX', ARGV[2])
            redis.call('DEL', deleted)
        else
            redis.call('PEXPIRE', data, ARGV[i + 3])
        end
    else
        redis.call('ZADD', KEYS[1], 'NX', now, ARGV[i])
    end
    redis.call('HDEL', KEYS[3], ARGV[i])
    redis.call('ZREM', KEYS[2], ARGV[i])
end
return 1
