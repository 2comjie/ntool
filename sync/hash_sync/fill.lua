-- KEYS: data hash, dirty fields set, load placeholder, negative cache
-- ARGV: field, token, payload, success, cache TTL, miss TTL
if redis.call('GET', KEYS[3]) ~= ARGV[2] then return 0 end
redis.call('DEL', KEYS[3])
if ARGV[4] ~= '1' then return 0 end
if ARGV[3] == '' then
    redis.call('SET', KEYS[4], '1', 'PX', ARGV[6])
else
    redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
    if redis.call('SCARD', KEYS[2]) == 0 and redis.call('PTTL', KEYS[1]) == -1 then
        redis.call('PEXPIRE', KEYS[1], ARGV[5])
    end
end
return 1
