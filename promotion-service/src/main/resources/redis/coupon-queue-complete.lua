-- Queue entries whose MySQL rows are written: drop their pending marks, acknowledge and delete
-- them, atomically. Done in separate calls, a failure in between could leave an entry
-- acknowledged but still in the stream, counted as queued forever.
-- KEYS[1] coupon:issue-stream   KEYS[1 + i] coupon:{id}:pending of entry i
-- ARGV[1] group   ARGV[1 + i] entry id   ARGV[1 + n + i] user id
local n = #KEYS - 1
for i = 1, n do
  redis.call('HDEL', KEYS[1 + i], ARGV[1 + n + i])
  redis.call('XACK', KEYS[1], ARGV[1], ARGV[1 + i])
  redis.call('XDEL', KEYS[1], ARGV[1 + i])
end
return n
