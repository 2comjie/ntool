for i = 2, #KEYS, 2 do
    redis.call('SET', KEYS[i], ARGV[i / 2 + 1])
    redis.call('ZADD', KEYS[1], ARGV[1], KEYS[i])
    redis.call('DEL', KEYS[i + 1])
end
return 1
