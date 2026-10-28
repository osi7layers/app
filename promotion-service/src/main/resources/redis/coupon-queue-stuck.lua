-- Queue entries handed out at least ARGV[2] ms ago and not acknowledged, oldest first.
-- XPENDING's IDLE filter, which Spring Data Redis 3.3 does not expose.
-- KEYS[1] coupon:issue-stream
-- ARGV[1] group   ARGV[2] min idle (ms)   ARGV[3] count
-- Returns a flat list: entry id, times handed out, entry id, times handed out, ...
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], 'IDLE', ARGV[2], '-', '+', ARGV[3])
local out = {}
for _, entry in ipairs(pending) do
  out[#out + 1] = entry[1]
  out[#out + 1] = tostring(entry[4])
end
return out
