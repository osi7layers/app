# #72 선착순 쿠폰 비동기 발급 (Redis Stream) 작업 개요

- 브랜치: `feat/72-coupon-async-stream` (main `b4a1660` 기준)
- 범위: promotion-service, `deploy/seed/promotion-coupon-reset-redis.sh`
- 프론트엔드(`frontend/`)는 수정하지 않음. 202 응답과 상태 폴링은 프론트 브랜치에서 따로 처리

## 목적

#44에서 수량 차감을 Redis로 옮겨 탈락 요청은 DB에 가지 않게 됐다. 남은 것은 당첨 요청이 MySQL `promotion_issue` INSERT를 기다리는 시간이다. 쿠폰 수량이 크고(만 장 단위) 짧은 시간에 소진되는 이벤트에서는 당첨자 수만큼 DB 쓰기가 몰린다. 당첨자를 Redis Stream에 넣고 바로 응답한 뒤, 워커가 묶어서 MySQL에 쓴다.

## 결정 사항

| 항목 | 결정 |
|---|---|
| 큐 | Redis Stream. 수량 차감과 큐 등록을 **같은 Lua 스크립트**에서 처리해 "수량은 줄었는데 메시지는 없는" 상태가 없다. Kafka·RabbitMQ는 별도 시스템이라 이중 쓰기 문제가 생기고 운영할 부품이 늘어난다 |
| 스위치 | `PROMOTION_ISSUE_STORE=redis-async`. 기본값은 그대로 `redis`라 **머지만으로는 동작이 바뀌지 않는다**. `redis` · `db`와 같은 코드로 부하 비교, 문제 시 환경변수로 즉시 되돌림 |
| 응답 | 202 `PENDING`. 결과는 상태 조회 API로 확인 (프론트가 폴링) |
| 원본 | MySQL이 최종 원본이다. 다만 처리 전 당첨은 Redis Stream에만 있으므로 **Redis AOF + 디스크 유지가 필수**이고, 운영은 Sentinel 이중화와 함께 쓴다 |
| 실패 처리 | 여러 번 실패한 항목은 한 번 더 기록을 시도하고, **MySQL이 행을 거부할 때만**(외래키 등) DLQ로 옮기고 수량 반납, 사용자 상태는 `FAILED`. MySQL에 닿지 않는 동안은 반납하지 않는다 |

## 동작 방식

```
POST /storefront/promotions/{id}/issue
 → 오픈 여부 확인 (pod 메모리 캐시, 3초)
 → Redis Lua 한 번: 이미 받았나? → 남았나? → 수량 -1, 발급자 추가, pending 기록, XADD coupon:issue-stream
 → 202 { promotionId, status: PENDING, pollAfterMs }

워커 (모든 pod, consumer group coupon-writer, consumer 이름 = pod 이름)
 → XREADGROUP 100건 → 한 트랜잭션으로 INSERT (유니크 위반이 섞이면 한 건씩, 중복은 이미 기록된 것으로 처리)
 → pending 삭제 · XACK · XDEL (Lua 하나로 원자 처리)
 → 10초마다: 30초 넘게 ACK 안 된 항목 회수(XPENDING IDLE + XCLAIM) → 다시 기록
             10번 넘게 전달된 항목 → 한 번 더 기록 시도
               MySQL이 거부 → coupon:issue-dlq(원자 이동), failed 기록, 수량 반납
               MySQL에 닿지 않음 → 그대로 둠 (다음 회차에 다시)
             pending 0이고 1시간 쉰 consumer(옛 pod) 삭제

GET /storefront/promotions/{id}/issue
 → Redis pending → MySQL → Redis failed 순서로 PENDING / ISSUED / FAILED / NONE
   (Redis 장애 중 MySQL에 행이 없으면 판단할 수 없으므로 503)
```

| Redis 키 | 내용 |
|---|---|
| `coupon:{id}:stock` · `users` · `pending` | #44와 같음. async에서 `pending`은 "큐에서 기다리는 당첨" |
| `coupon:{id}:failed` | 큐 처리에 실패해 반납된 사용자 → 시각 (상태 `FAILED`) |
| `coupon:issue-stream` | 기록 대기 중인 당첨 (`promotionId`, `userId`, `claimedAt`). 기록 후 삭제되므로 길이 = 남은 일 |
| `coupon:issue-dlq` | 포기한 항목 (원래 필드 + `sourceId`, `deliveries`, `deadAt`) |

- `coupon:issue-stream`은 `{id}` 해시 태그 밖이라 이 스크립트는 **단일 Redis(또는 Sentinel)** 에서만 동작한다. Redis Cluster로 갈 때는 쿠폰별 Stream으로 나눠야 한다.
- XADD에 `MAXLEN`을 두지 않는다. 길이 제한은 처리 여부와 상관없이 오래된 항목을 지워, 아직 기록하지 않은 당첨이 사라질 수 있다. 워커가 기록 후 지운다.
- 기록 순서(MySQL 커밋 → pending 삭제 → ACK)와 상태 조회 순서(pending → MySQL)가 맞물려 있어, 그 사이에 조회해도 `NONE`으로 보이지 않는다.
- `StaleClaimSettler`(#44의 시간 초과 발급 되돌림)는 동기 `redis`에서만 돈다. async의 pending은 큐에서 기다리는 당첨이라 되돌리면 초과 발급된다. async에서는 큐 회수와 DLQ가 그 역할을 한다.

## 변경 내용

| 커밋 | 내용 |
|---|---|
| `1a12944` | `redis-async` 스위치. Redis 사용 판단을 `usesRedis()`로 모아 async에서도 `issued_count` 동기화 · 목록 실시간 수량 · 쿠폰 생성/삭제 반영 · health가 동작. Settler는 동기 redis에서만 |
| `3292936` | `coupon-claim-async.lua`: 차감과 XADD를 한 번에 |
| `9aff81e` | `CouponIssueWorker`: 일괄 INSERT, 중복 허용, ACK·삭제, 그룹 자동 생성 |
| `c274547` | 회수(XPENDING + XCLAIM, Spring Data Redis 3.3에 XAUTOCLAIM API가 없음), DLQ, 수량 반납, 옛 consumer 정리 |
| `b7c38d7` | async 모드 발급 응답 202 `PENDING` |
| `45cf922` | 발급 상태 조회 API, 로그인 필수 |
| `9dddeeb` | 쿠폰 목록 `issueStatus` |
| `62c0d8e` | Prometheus 지표, 큐 지연 시 `pollAfterMs` 증가 |
| `4a98a27` | 통합 테스트 |
| `5422b48` | 이 문서, QA 초기화 스크립트가 큐도 지우도록 |
| `d4fc67a` | **검수 수정**: 한도 넘은 항목도 한 번 더 시도, MySQL에 닿지 않으면 반납 안 함. 완료 · DLQ 이동 원자 처리, 회수 대상을 IDLE 필터로 조회 |
| `48b9ccd` | **검수 수정**: Redis 장애 중 상태 조회가 처리 중인 발급을 `NONE`으로 답하지 않고 503 |
| `bfb09ab` | **검수 수정**: 대기 시간을 Redis 서버 시각 기준으로 계산 |
| (이 커밋) | 문서 갱신 |

### 검수에서 발견해서 고친 것

1. **MySQL 장애 뒤 일괄 반납 (`d4fc67a`)**: 전달 횟수만 보고 DLQ로 보냈기 때문에, MySQL이 5분(30초 × 10회) 넘게 멈췄다 돌아오면 그동안 큐에 있던 **정상 발급이 전부 `FAILED`로 반납**됐다. 이제 한도를 넘은 항목도 한 번 더 기록을 시도하고, MySQL이 행을 거부할 때만 포기한다. MySQL에 닿지 않으면 남겨 둔다.
2. **완료 처리 중간 실패 (`d4fc67a`)**: pending 삭제, XACK, XDEL을 따로 호출해서 XACK 뒤 XDEL이 실패하면 항목이 Stream에 영구히 남았다. 대기열 길이 · 대기 시간 지표가 계속 올라가 `pollAfterMs`가 3초에 고정되고 알림이 울린다. Lua 하나로 묶었다. DLQ 이동도 같은 이유로 묶었다.
3. **회수 대상 조회 (`d4fc67a`)**: XPENDING 앞쪽 100건 중에서 idle인 것만 골라, 처리 중인 항목이 많을 때는 그 뒤의 멈춘 항목을 찾지 못했다. Spring Data Redis 3.3에 IDLE 옵션이 없어 Lua로 `XPENDING ... IDLE`을 호출한다.
4. **Redis 장애 중 상태 조회 (`48b9ccd`)**: pending을 확인하지 못해도 `NONE`으로 답해, 프론트가 처리 중인 사용자에게 실패를 보여 줄 수 있었다. MySQL에 행이 없으면 503으로 답하고 프론트는 간격을 늘려 다시 묻는다.
5. **시계 차이 (`bfb09ab`)**: Stream ID(Redis 서버 시각)를 Pod 시각과 비교해 대기 시간이 틀어질 수 있었다.

## 프론트 연동 안내

경로는 게이트웨이 기준 `/promotion/...`이다.

### 발급 응답

`POST /promotion/storefront/promotions/{promotionId}/issue` (토큰 필요)

| 결과 | HTTP | 본문 / `code` |
|---|---|---|
| 접수 (redis-async) | **202** | `{ "promotionId": 6, "status": "PENDING", "issueId": null, "issuedAt": null, "pollAfterMs": 1000 }` |
| 발급 (redis, db) | 201 | 발급 정보 (기존과 같음) |
| 이미 받음 · 매진 · 오픈 전 · Redis 장애 · 없는 쿠폰 | 409 · 409 · 400 · 503 · 404 | `PRM-6003` · `PRM-6002` · `PRM-6001` · `PRM-6004` · `ERR-0404` (기존과 같음) |

**프론트는 201과 202를 모두 처리해야 한다.** 202를 처리하는 프론트가 배포되기 전에는 `redis-async`를 켜지 않는다.

### 상태 조회

`GET /promotion/storefront/promotions/{promotionId}/issue` (토큰 필요, 본인 것만)

```json
{ "promotionId": 6, "status": "ISSUED", "issueId": 5531, "issuedAt": "2026-10-28T03:00:01Z", "pollAfterMs": 0 }
```

| `status` | 뜻 | 프론트 처리 |
|---|---|---|
| `PENDING` | 접수됨, 처리 중 | `pollAfterMs` 뒤 다시 조회 |
| `ISSUED` | 발급 완료 | "발급 완료" 알림, 폴링 중단 |
| `FAILED` | 처리 실패, 수량 반납됨 | "발급 실패" 알림, 폴링 중단 (다시 시도 가능) |
| `NONE` | 발급 없음 | 202 직후 잠깐 보일 수 있어 3번까지는 계속, 그래도 `NONE`이면 실패로 안내 |
| (HTTP 503 `PRM-6004`) | Redis 장애로 처리 중인지 알 수 없음 | 간격을 2배로 늘려 계속 조회 |

폴링 규칙 (프론트와 합의):
- 202를 받았을 때만 시작한다. 간격은 `pollAfterMs` + 무작위 0~300ms.
- 최대 30초. 넘으면 "처리가 지연되고 있어요"로 안내하고 중단한다.
- 503 · 429면 간격을 2배로, 401이면 중단 후 로그인 안내. 탭이 숨겨지면 멈춘다.
- `pollAfterMs`는 평소 1000이고, 큐가 밀리면 서버가 2000 · 3000으로 늘린다 (가장 오래 기다린 항목 5초마다 한 단계).

### 쿠폰 목록

`GET /promotion/storefront/promotions` 항목에 `issueStatus`가 추가됐다 (`PENDING` / `ISSUED` / `FAILED` / `NONE`, 비로그인은 `NONE`). `claimed`는 그대로 있고 `issueStatus == ISSUED`와 같다. 새로고침했을 때 `PENDING`인 카드는 폴링을 다시 시작하면 된다.

## 설정

| 환경변수 | 기본값 | 설명 |
|---|---|---|
| `PROMOTION_ISSUE_STORE` | `redis` | `redis` · `redis-async` · `db` |
| `PROMOTION_ISSUE_STATUS_POLL_AFTER` | `1s` | `PENDING` 응답의 기본 `pollAfterMs` (밀리면 최대 3배) |
| `PROMOTION_ISSUE_WORKER_BATCH_SIZE` | `100` | 한 트랜잭션에 기록하는 항목 수 |
| `PROMOTION_ISSUE_WORKER_POLL_INTERVAL` | `200ms` | 큐가 비었을 때 다시 읽기까지 대기. blocking read는 Redis 명령 시간 초과(1s)와 충돌해 쓰지 않음 |
| `PROMOTION_ISSUE_WORKER_RECLAIM_INTERVAL` | `10s` | 회수 · DLQ · consumer 정리 주기 |
| `PROMOTION_ISSUE_WORKER_CLAIM_IDLE` | `30s` | 이 시간 넘게 ACK 안 된 항목을 회수 |
| `PROMOTION_ISSUE_WORKER_MAX_DELIVERIES` | `10` | 넘으면 한 번 더 기록을 시도하고, MySQL이 거부하면 DLQ + 반납. **MySQL에 닿지 않는 동안은 몇 번이든 반납하지 않는다** |
| `PROMOTION_ISSUE_WORKER_CONSUMER_EXPIRY` | `1h` | pending 0인 옛 pod consumer 삭제 기준 |

## 지표 (`:9000/actuator/prometheus`)

| 지표 | 뜻 | 알림 예시 |
|---|---|---|
| `coupon_issue_queue_length` | 아직 MySQL에 기록 안 된 당첨 | |
| `coupon_issue_queue_pending` | 그중 워커가 가져갔고 ACK 안 된 것 | |
| `coupon_issue_queue_oldest_age_seconds` | 가장 오래 기다린 항목의 대기 시간 | 30초 이상 1분 지속 |
| `coupon_issue_dead_letters` | DLQ 항목 수 | 1 이상 |
| `coupon_issue_written_total` · `_reclaimed_total` · `_dead_lettered_total` | 기록 · 회수 · 포기 누적 | |

Redis는 최대 1초에 한 번만 읽는다 (스크레이프와 폴링이 몰려도 한 호출만 갱신).

## 운영

### DLQ 확인
```bash
kubectl exec -it <redis-pod> -- sh -c 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli XRANGE coupon:issue-dlq - + COUNT 20'
```
DLQ 항목은 MySQL이 행을 거부한 것(쿠폰 삭제로 외래키 위반 등)이다. 사용자는 이미 `FAILED`로 안내되고 수량도 반납됐다. 원인을 확인한 뒤 `XDEL`로 지운다.

### 큐 상태
```bash
kubectl exec -it <redis-pod> -- sh -c 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli XLEN coupon:issue-stream; REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli XINFO GROUPS coupon:issue-stream'
```

### 되돌리기
`PROMOTION_ISSUE_STORE=redis`로 바꾸면 동기 방식으로 돌아간다. **큐가 비었는지(`XLEN` 0) 확인한 뒤** 바꾼다. 동기 모드에서는 워커가 돌지 않아 남은 항목이 기록되지 않는다.

### 회차별 초기화 (QA)
#44와 같은 순서다. `promotion-coupon-reset-redis.sh`가 이제 `coupon:issue-stream`, `coupon:issue-dlq`도 지운다. 남아 있으면 초기화 뒤 워커가 이전 당첨을 다시 기록하기 때문이다. **큐가 빈 뒤에** 실행한다. 큐를 지우면 워커가 그룹을 스스로 다시 만든다.

## 검증

### 테스트
- promotion-service 테스트 51개 통과 (단위 26, 통합 25: async 12 · 동기 13). 통합은 Testcontainers MySQL 8.0 · Redis 7.4. async 통합 테스트는 3회 반복해서 모두 통과
- 워커 단위 테스트: 한도 넘은 항목이 마지막 시도에 기록되면 완료, MySQL이 거부하면 DLQ + 반납, MySQL에 닿지 않으면 아무것도 반납하지 않음
- async 통합 테스트:
  - 50장에 300명 동시 → `PENDING` 50, 매진 250, 최종 행 50, 재고 0, 중복 0
  - 워커가 멈춘 동안 10장에 40명 → `PENDING` 10, 매진 30, 상태 `PENDING` 유지 → 워커 재시작 후 10건 기록, 기록 수 + 재고 = 수량
  - 같은 사용자 재요청 409, 상태 `NONE` → `PENDING` → `ISSUED`
  - 다시 전달된 항목은 행을 하나만 남김, 배치 안의 중복이 나머지를 막지 않음
  - 죽은 consumer가 들고 있던 항목 회수 후 기록, 계속 실패하는 항목 DLQ + 반납 + `FAILED`, 옛 consumer 정리
  - 목록 `issueStatus` (`PENDING` · `ISSUED` · `FAILED` · `NONE`, 비로그인 `NONE`), 지표 노출
- 동기 `redis` 테스트(기존)는 그대로 통과. 동기 모드에서는 워커가 돌지 않는다

### 아직 안 한 것
- 게이트웨이 경유 확인과 dev 반영 (gitops 스위치 이후)
- 부하 비교: 같은 k6 시나리오로 `db` → `redis` → `redis-async`

## gitops 반영 항목

- **promotion-service 환경변수**: `PROMOTION_ISSUE_STORE=redis-async` (dev 먼저). 나머지는 기본값으로 충분
- **redis-coupon**: AOF(`appendfsync always`)와 PVC를 **유지**한다. 처리 전 당첨이 Stream에만 있다. 운영은 Sentinel 이중화와 함께
- **ApisixRoute**: `GET /promotion/storefront/promotions/*/issue`가 promotion-service로 가는지 확인 (기존 `/promotion` 경로 규칙에 포함되는지)
- **요청 제한**: 상태 조회 경로 limit-req (예: 사용자 IP당 초당 3건)
- **모니터링**: 위 지표 대시보드, `oldest_age` · `dead_letters` 알림

## 알려진 제약

- 처리 전 당첨은 Redis에만 있다. Redis 데이터가 통째로 사라지면 그 당첨은 사용자에게 `PENDING`을 준 채 사라진다(수량도 MySQL 기준으로 다시 채워져 초과 발급은 없다). AOF와 Sentinel이 전제다. Sentinel 전환 시 비동기 복제로 마지막 몇 ms가 사라질 수 있어, 필요하면 Lua 뒤에 `WAIT`으로 복제 확인 후 응답하는 것을 이후에 추가한다.
- 큐에 항목이 남은 상태에서 쿠폰 키만 지우면(`evict`, 수동 삭제) 다시 채울 때 큐의 당첨이 빠져 그만큼 초과 발급될 수 있다. 쿠폰 삭제(`evict`)는 쿠폰 자체가 없어져 해당 항목이 DLQ로 가므로 문제없고, 수동으로 지울 때는 큐가 빈 뒤에 한다.
- 단일 Redis 전제 (위 "해시 태그" 참고).
- 동기 `redis`에서 `redis-async`로 바꿀 때: 동기 모드의 시간 초과로 남은 pending(드묾)은 async에서 정리되지 않는다(정리기는 동기 모드에서만 돈다). 바꾸기 전에 `coupon:{*}:pending`이 비어 있는지 확인한다.
- 큐 지표는 Redis를 1초에 한 번만 읽으므로 최대 1초 늦다.

## 남은 것

- 프론트: 202 처리, 상태 폴링, 목록 `issueStatus` (프론트 담당)
- gitops: dev 스위치, 라우트 확인, limit-req (지은 님 · 예찬 님)
- k6: 발급 + 폴링 시나리오, `db` · `redis` · `redis-async` 비교 (재학 님)
- Redis Sentinel 이중화 (예찬 님), 이후 `WAIT` 검토
