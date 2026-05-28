local key = KEYS[1]
local zset_key = KEYS[2]
local value = ARGV[1]
local current_ts = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

-- 1. Clean up old keys from the ZSET before validation
redis.call("ZREMRANGEBYSCORE", zset_key, "-inf", current_ts)

-- 2. Check if the key exists in the tombstone list (ZSET)
local score = redis.call("ZSCORE", zset_key, key)

-- 3. Set the value strictly if the key is not found in the ZSET
if not score then
	if ttl and ttl > 0 then
		redis.call("SET", key, value, "EX", ttl)
	else
		redis.call("SET", key, value)
	end

	return 1 -- Success: Key was set
end

return 0 -- Failure: Write blocked by the delete lock (tombstone)