-- Claims one first-come coupon for a user, atomically.
-- KEYS[1] coupon:{id}:stock   KEYS[2] coupon:{id}:users   ARGV[1] user id
-- Returns 0 claimed, 1 sold out, 2 already claimed, -1 not loaded (rebuild from MySQL first).
if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
  return 2
end
if tonumber(redis.call('GET', KEYS[1])) <= 0 then
  return 1
end
redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])
return 0
