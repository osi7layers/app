#!/bin/sh
# Clears first-come coupon state from Redis. Run it right after promotion-coupon-reset.sql:
# otherwise Redis still holds the old stock and claimers, and the reset coupons stay sold out.
#
# Every coupon:{id}:* key is removed, not only the demo ones. That is safe: promotion-service
# rebuilds a coupon's keys from MySQL on its next claim.
#
# redis-async: the issue queue and its dead letters are removed too, or the worker would write
# claims from before the reset after it. The workers create the queue again by themselves.
# Run it once the queue is empty (coupon_issue_queue_length 0), or those claims are lost.
#
#   compose: docker compose exec -T -e REDIS_PASSWORD="$REDIS_PASSWORD" redis sh < deploy/seed/promotion-coupon-reset-redis.sh
#   k8s:     kubectl exec -i <redis-pod> -- env REDIS_PASSWORD="$REDIS_PASSWORD" sh < deploy/seed/promotion-coupon-reset-redis.sh
set -eu
cli() {
  redis-cli -a "$REDIS_PASSWORD" --no-auth-warning "$@"
}
cli --scan --pattern 'coupon:{*' | xargs -r redis-cli -a "$REDIS_PASSWORD" --no-auth-warning DEL
cli DEL coupon:issue-stream coupon:issue-dlq > /dev/null
echo "coupon keys left: $(cli --scan --pattern 'coupon:*' | wc -l)"
