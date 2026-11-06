for i = 2, #KEYS do
    if redis.call('GET', KEYS[i]) == ARGV[(i - 2) * 2 + 2] then
        redis.call('ZREM', KEYS[1], KEYS[i])
        redis.call('PEXPIRE', KEYS[i], ARGV[(i - 2) * 2 + 3])
    elseif redis.call('ZSCORE', KEYS[1], KEYS[i]) then
        redis.call('ZADD', KEYS[1], ARGV[1], KEYS[i])
    end
end
return 1
