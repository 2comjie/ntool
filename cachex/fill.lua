if redis.call('GET', KEYS[2]) ~= ARGV[1] then
    return 0
end
if ARGV[2] ~= '' then
    redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
end
redis.call('DEL', KEYS[2])
return 1
