local key = KEYS[1]
local zset_key = KEYS[2]
local current_ts = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local expire_ts = current_ts + ttl

-- 1. Delete the primary key
redis.call("DEL", key)

-- 2. Add the deleted key to the ZSET with the expiration timestamp as the score
redis.call("ZADD", zset_key, expire_ts, key)

-- 3. Extend the TTL of the entire ZSET if necessary
local current_zset_ttl = redis.call("TTL", zset_key)
if current_zset_ttl < ttl then
-- If ZSET TTL is less than the added key's TTL (or ZSET has no TTL, returning -1), set a new TTL
	redis.call("EXPIRE", zset_key, ttl)
end

-- 4. Clean up the ZSET by removing old, expired entries (where score <= current_ts)
redis.call("ZREMRANGEBYSCORE", zset_key, "-inf", current_ts)

return 1