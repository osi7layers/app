-- Moves one queue entry to the dead-letter stream: copy, acknowledge and delete, atomically,
-- so a retry never copies it twice.
-- KEYS[1] coupon:issue-stream   KEYS[2] coupon:issue-dlq
-- ARGV[1] group   ARGV[2] entry id   ARGV[3..] field, value, field, value, ...
local fields = {}
for i = 3, #ARGV do
  fields[#fields + 1] = ARGV[i]
end
redis.call('XADD', KEYS[2], '*', unpack(fields))
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
return 1
