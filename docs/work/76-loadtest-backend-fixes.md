# #76 부하 시험 백엔드 문제 3건 작업 개요

- 브랜치: `fix/76-loadtest-backend-fixes` (main `9c9648b` 기준)
- 범위: order-service, promotion-service, common-lib(common-spring)
- 프론트엔드(`frontend/`)는 수정하지 않음. 요청·응답 형식은 그대로다

## 1. 주문 목록을 로그인 사용자 주문만 조회 (order-service)

### 문제

- `GET /api/orders`, `GET /api/orders/all`이 `orderRepository.findAll()`로 **모든 사용자의 주문**을 돌려줬다.
- 주문마다 product-service를 한 번씩 호출했다 (부하 시험에서 요청 하나에 1,875번).
- 페이지 응답의 전체 개수를 `orderDtos.size()`(현재 페이지 건수)로 넣어 전체 페이지 수가 틀렸다.

### 처음 계획(cart 경유)을 쓰지 않은 이유

| 확인한 것 | 결과 |
|---|---|
| 주문 생성 요청 (`checkout/page.tsx`) | `orderDate, orderDesc, orderFee, productId, promotionId`만 보낸다. cart를 보내지 않아 `orders.cart_id`는 항상 NULL |
| `carts.user_id` | `BIGINT`. 로그인 사용자 값(`AuthenticationUtils.currentUserId()`)은 Keycloak `sub`(UUID 문자열)라 형식이 다르다 |
| 감사 컬럼 | `AbstractMappedEntity`에 `createdAt`/`updatedAt`만 있고 작성자는 없다 |

즉 기존 데이터로는 주문의 주인을 알 수 없어 `orders`에 사용자 컬럼을 추가했다.

### 결정 사항

| 항목 | 결정 |
|---|---|
| 컬럼 | `orders.user_id VARCHAR(64) NULL` + 인덱스 `idx_orders_user_id` (Liquibase `changelog-0003.sql`). auth-service `keycloak_user_id VARCHAR(64)`와 같은 형식 |
| 저장 | `save()`에서 `AuthenticationUtils.requireUserId()`로 채운다. 요청 본문에서는 받지 않는다 |
| 수정 | `update()` 두 메서드 모두 DB의 기존 `user_id`를 유지한다 (DTO에 없는 값이라 그냥 저장하면 NULL로 지워진다) |
| 응답 | `OrderDto`에 `userId`를 넣지 않는다. 응답 형식 변화 없음 |
| 권한 | `ADMIN` 권한이 있으면 전체, 그 외는 자기 주문만 (관리자 화면은 그대로 동작) |
| product 호출 | 요청 안에서 `Map<productId, ProductDto>`로 묶어 같은 상품은 한 번만 호출. product-service 변경 없음 |
| 페이지 | 전체 개수를 `Page.getTotalElements()`로 |

### 영향 확인

- 다른 서비스(shipping 등)는 `/api/orders/{id}` 단건 조회만 부른다. 목록 API를 부르는 서비스 없음.
- `PaymentServiceImpl`은 주문을 읽기만 한다.
- `user_id`는 NULL 허용이고 `ddl-auto: validate`는 남는 컬럼을 무시하므로, 이전 이미지로 롤백해도 기동된다.
- DB 권한: prod 계정에 `ALTER, INDEX`, dev 계정은 ALL.

## 2. Redis 재연결 간격 최대 1초 (promotion-service)

### 문제

redis-coupon pod 재시작 시 쿠폰 발급이 9~17초 멈췄다. 새 pod는 약 7~8초면 뜨는데, Lettuce 기본 재연결 backoff가 최대 30초까지 늘어나 그 사이 재시도를 놓쳤다.

### 변경

- `config/RedisReconnectConfig` 추가: `Delay.exponential(100ms, 1s, 2)`로 재연결 간격 상한 1초.
- `LettuceClientConfigurationBuilderCustomizer`로 `ClientResources`를 새로 만들면 Boot가 종료 시 정리하지 않는다. 그래서 Boot가 만들고 관리하는 `ClientResources`에 `ClientResourcesBuilderCustomizer`로 적용했다.

## 3. 4xx 경고 로그에서 스택 트레이스 제거 (common-lib)

- `ApiExceptionHandler.respond()`의 4xx `log.warn`에서 마지막 인자 `ex`를 뺐다. `cause=` 메시지는 남으므로 원인은 한 줄로 확인할 수 있다.
- 5xx `log.error`는 스택 트레이스를 그대로 남긴다.
- 품절(409, `INV-3002`) 등 정상적인 업무 오류가 몰릴 때 로그량이 크게 줄어든다.

## 빌드·테스트

| 대상 | 결과 |
|---|---|
| order-service | `mvn -pl order-service -am compile` 성공 (테스트 없음) |
| promotion-service | `mvn -pl promotion-service -am test` 51건 통과 (Redis 통합 테스트 포함) |
| common-spring | `mvn -pl common-lib/common-spring -am test` 통과 |

## 배포 시 확인

### 다시 빌드할 이미지

common-spring을 쓰는 서비스는 모두 다시 빌드해야 3번이 반영된다.
auth, favourite, notification, media, inventory, order, rating, promotion, product, search, tax, shipping

### dev 확인 방법

1. **order**: 배포 후 order-service 기동 로그에서 Liquibase `order-mysql-ddl-0003` 적용 확인. USER 두 명으로 각각 주문 후 `GET /api/orders`에 자기 주문만 나오는지, ADMIN 계정은 전체가 나오는지 확인. order 로그의 product 호출 수가 상품 종류 수로 줄었는지 확인
2. **promotion**: 쿠폰 발급 부하 중 `kubectl rollout restart` 또는 pod 삭제로 redis-coupon 재시작 → 발급이 멈춘 시간이 pod 기동 시간(약 7~8초) 근처인지 확인
3. **common**: 품절 409 발생 시 로그가 `ApiError uri=... status=409 ...` 한 줄인지 확인

## 남은 문제 (이번 범위 밖)

- 이번 배포 전 주문, 롤링 배포 중 이전 pod로 들어온 주문은 `user_id`가 NULL이라 ADMIN 목록에만 보인다.
- `GET /api/orders/{id}`는 여전히 USER가 다른 사용자 주문도 조회할 수 있다. shipping-service가 이 API를 부르므로 따로 다룬다.
- 장바구니 저장이 주문까지 함께 저장(cascade)하는 경로는 원래부터 주문의 `productId` 등을 잃는다. 프론트에서 쓰지 않는 경로라 그대로 뒀다.
