-- Make a sale live: load the stock and rebuild the buyers set in one atomic step.
-- KEYS[1] = sale:{id}:stock   KEYS[2] = sale:{id}:buyers
-- ARGV[1] = remaining (PostgreSQL)   ARGV[2..n] = user_ids that already have a live order
-- Returns 1 started, 0 already live (nothing changed: resetting a live sale would oversell).
--
-- One script on purpose: setting the stock first and rebuilding the set afterwards left a gap
-- in which the first buyers' reservations were wiped from the set, at the busiest moment of a
-- sale. They could reserve a second unit that the DB then rejects, so Redis undersold.
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('DEL', KEYS[2])
for i = 2, #ARGV do
  redis.call('SADD', KEYS[2], ARGV[i])
end
redis.call('SET', KEYS[1], ARGV[1])
return 1
