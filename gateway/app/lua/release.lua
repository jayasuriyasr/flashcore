-- Undo a reservation: give the unit back and forget the buyer.
-- KEYS[1] = sale:{id}:stock   KEYS[2] = sale:{id}:buyers   ARGV[1] = user_id
-- Returns 1 if a reservation was released, 0 if there was nothing to release.
--
-- Stock is only given back when the user really was in the buyers set, so running
-- this twice for the same reservation cannot create a unit out of thin air.
-- A missing stock key means the sale was ended; it is not recreated.
if redis.call('SREM', KEYS[2], ARGV[1]) == 1 then
  if redis.call('EXISTS', KEYS[1]) == 1 then
    redis.call('INCR', KEYS[1])
  end
  return 1
end
return 0
