-- Reserve one unit for one user.
-- KEYS[1] = sale:{id}:stock   KEYS[2] = sale:{id}:buyers   ARGV[1] = user_id
-- Returns 1 reserved, 0 sold out / sale not started, -1 already bought.
--
-- Redis runs a script start to finish with nothing in between, so this
-- check-then-decrement cannot race with another buyer. No lock needed.
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
  return -1
end
local stock = tonumber(redis.call('GET', KEYS[1]) or '0')
if stock <= 0 then
  return 0
end
redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])
return 1
