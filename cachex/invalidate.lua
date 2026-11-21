redis.call('DEL', KEYS[1], KEYS[2])
return redis.call('PUBLISH', ARGV[1], ARGV[2])
