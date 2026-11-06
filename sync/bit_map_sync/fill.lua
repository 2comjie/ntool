-- KEYS: bitmap, load placeholder
-- ARGV: token, raw bytes, success, TTL milliseconds
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[2])
if ARGV[3] ~= '1' then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[4])
return 1
