-- Lower the stock counter to ARGV[1] only if it is currently higher. Never raises it.
-- KEYS[1] = sale:{id}:stock   ARGV[1] = target (PostgreSQL remaining)
-- Returns how many units were removed (0 if nothing changed).
--
-- Used by reconcile: Redis above the DB is the unsafe direction (it could admit
-- buyers the DB will reject), so it is corrected. Redis below the DB is the safe
-- direction (we undersell) and is left alone while the sale is live.
local current = tonumber(redis.call('GET', KEYS[1]) or '-1')
local target = tonumber(ARGV[1])
if current > target then
  redis.call('SET', KEYS[1], target)
  return current - target
end
return 0
