-- Claims one first-come coupon for a user and queues it for the MySQL writer, atomically.
-- KEYS[1] coupon:{id}:stock   KEYS[2] coupon:{id}:users   KEYS[3] coupon:{id}:pending
-- KEYS[4] coupon:issue-stream
-- ARGV[1] user id   ARGV[2] now (epoch millis)   ARGV[3] promotion id
-- Returns 0 claimed, 1 sold out, 2 already claimed, -1 not loaded (rebuild from MySQL first).
-- The claim and its queue entry are written together, so stock never drops without a message
-- for the worker. The pending hash marks the claim as queued until its MySQL row is written.
-- No MAXLEN on XADD: trimming would drop entries the worker has not written yet; the worker
-- deletes each entry once it is written.
-- KEYS[4] is outside the {id} hash tag, so this needs a single Redis (or Sentinel), not Cluster.
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
redis.call('HSET', KEYS[3], ARGV[1], ARGV[2])
redis.call('XADD', KEYS[4], '*', 'promotionId', ARGV[3], 'userId', ARGV[1], 'claimedAt', ARGV[2])
return 0
