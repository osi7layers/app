import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { couponApi } from "@ecommerce/lib/api";
import type { ApiResponse } from "@ecommerce/lib/types";
import type { CouponEvent, IssueResult } from "@/data/couponMock";

// 실제 API 사용 중. true 로 바꾸면 비로그인 사용자도 발급 버튼을 누를 수 있다 (mock 시절 동작).
export const COUPON_MOCK_MODE = false;

// GET /promotion/storefront/promotions 응답 항목 (promotion-service StorefrontCouponVm)
interface StorefrontCoupon {
  promotionId: number;
  name: string;
  description?: string;
  discountType: "FIXED" | "PERCENTAGE";
  discountAmount?: number;
  discountPercentage?: number;
  minimumOrderPurchaseAmount?: number;
  issueLimit: number;
  issuedCount: number;
  startDate: string;
  endDate: string;
  claimed: boolean;
  issueStatus?: "PENDING" | "ISSUED" | "FAILED" | "NONE";
}

async function fetchCoupons(): Promise<CouponEvent[]> {
  const res = await couponApi.getAll();
  const list = (res.data as ApiResponse<StorefrontCoupon[]>).data ?? [];
  return list.map((c) => ({
    promotionId: c.promotionId,
    name: c.name,
    description: c.description,
    // 정률 쿠폰은 discountAmount 가 null 이라 0 으로 둔다. 카드는 discountPercentage 가 있으면 % 로 보여 준다.
    discountAmount: c.discountAmount ?? 0,
    discountType: c.discountType,
    discountPercentage: c.discountPercentage,
    minimumOrderPurchaseAmount: c.minimumOrderPurchaseAmount,
    issueLimit: c.issueLimit,
    issuedCount: c.issuedCount,
    startDate: c.startDate,
    endDate: c.endDate,
    claimed: c.claimed,
    issueStatus: c.issueStatus,
  }));
}

// 백엔드 ErrorCode (common-lib) 의 code / message 키를 화면 결과로 바꾼다.
const ISSUE_ERRORS: Record<string, IssueResult> = {
  "PRM-6001": "notOpen",
  "promotion.coupon.not.open": "notOpen",
  "PRM-6002": "soldOut",
  "promotion.coupon.sold.out": "soldOut",
  "PRM-6003": "alreadyClaimed",
  "promotion.coupon.already.claimed": "alreadyClaimed",
};

// 202 응답과 상태 조회 API(GET .../{id}/issue)의 본문 (promotion-service CouponClaimStatusVm)
interface ClaimStatus {
  promotionId: number;
  status: "PENDING" | "ISSUED" | "FAILED" | "NONE";
  issueId: number | null;
  issuedAt: string | null;
  pollAfterMs: number;
}

// 폴링 규칙 (광렬님과 합의, docs/work/72-coupon-async-stream.md)
const POLL_DEFAULT_MS = 1000; // pollAfterMs 가 없거나 0일 때 (NONE 응답 등)
const POLL_JITTER_MS = 300; // 여러 사용자가 같은 순간에 몰리지 않게 0~300ms 를 더한다
const POLL_TIMEOUT_MS = 30_000; // 이만큼 보이는 동안 조회해도 결과가 없으면 "지연" 안내 후 중단
const POLL_MAX_NONE = 3; // NONE 은 202 직후 잠깐 보일 수 있어 3번까지는 계속 조회한다

type ErrorBody = { code?: string; message?: string };
type AxiosLikeError = { response?: { status?: number; data?: ErrorBody } };

function errorResult(e: unknown): IssueResult {
  // axios 오류면 response.data 에 ApiResponse 오류 본문이 있다.
  const body = (e as AxiosLikeError).response?.data;
  return ISSUE_ERRORS[body?.code ?? ""] ?? ISSUE_ERRORS[body?.message ?? ""] ?? "error";
}

// 탭이 숨겨져 있으면 다시 보일 때까지 기다린다. 기다린 시간(ms)을 돌려준다.
function waitUntilVisible(signal?: AbortSignal): Promise<number> {
  if (typeof document === "undefined" || !document.hidden) return Promise.resolve(0);
  const hiddenAt = Date.now();
  return new Promise((resolve) => {
    const done = () => {
      if (document.hidden && !signal?.aborted) return;
      document.removeEventListener("visibilitychange", done);
      signal?.removeEventListener("abort", done);
      resolve(Date.now() - hiddenAt);
    };
    document.addEventListener("visibilitychange", done);
    signal?.addEventListener("abort", done);
  });
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener("abort", () => { clearTimeout(timer); resolve(); }, { once: true });
  });
}

// 202 를 받은 뒤 상태 조회 API 를 결과가 나올 때까지 묻는다.
// 숨겨진 탭에서 기다린 시간은 30초에 넣지 않는다 (다시 보이면 이어서 조회).
async function pollIssueStatus(promotionId: number, firstDelayMs: number, signal?: AbortSignal): Promise<IssueResult> {
  let deadline = Date.now() + POLL_TIMEOUT_MS;
  let delay = firstDelayMs || POLL_DEFAULT_MS;
  let noneCount = 0;

  while (!signal?.aborted) {
    await sleep(delay + Math.floor(Math.random() * POLL_JITTER_MS), signal);
    deadline += await waitUntilVisible(signal);
    if (signal?.aborted) break;
    if (Date.now() > deadline) return "delayed";

    try {
      const { data } = await couponApi.issueStatus(promotionId);
      const claim = data as ClaimStatus;
      if (claim.status === "ISSUED") return "success";
      if (claim.status === "FAILED") return "failed";
      if (claim.status === "NONE" && ++noneCount > POLL_MAX_NONE) return "failed";
      delay = claim.pollAfterMs || POLL_DEFAULT_MS;
    } catch (e) {
      const status = (e as AxiosLikeError).response?.status;
      if (status === 401) return "loginRequired";
      // 429(요청이 많음)와 503(PRM-6004, Redis 장애로 확인 불가)은 간격을 2배로 늘려 계속 조회한다.
      // 그 밖의 오류(네트워크 등)도 접수는 된 상태라 실패로 보이지 않고 같은 간격으로 다시 묻는다.
      if (status === 429 || status === 503) delay *= 2;
    }
  }
  return "delayed";
}

// 201 이면 바로 발급 완료, 202 면 onPending 을 부른 뒤 결과가 나올 때까지 상태를 조회한다.
async function issueCoupon(promotionId: number, onPending?: () => void, signal?: AbortSignal): Promise<IssueResult> {
  try {
    const res = await couponApi.issue(promotionId);
    if (res.status !== 202) return "success";
    onPending?.();
    return await pollIssueStatus(promotionId, (res.data as ClaimStatus).pollAfterMs, signal);
  } catch (e) {
    return errorResult(e);
  }
}

export const COUPONS_KEY = ["coupons"] as const;

export function useCoupons() {
  return useQuery({
    queryKey: COUPONS_KEY,
    queryFn: fetchCoupons,
  });
}

// 체크아웃에서 고를 수 있는 쿠폰: 내가 발급받았고 지금 사용 기간 안에 있는 것.
// 목록 API는 이미 사용한 쿠폰도 claimed 로 주므로, 사용한 쿠폰은 주문할 때 백엔드가 거부한다.
export function isUsableCoupon(coupon: CouponEvent, now: number): boolean {
  return coupon.claimed && now >= Date.parse(coupon.startDate) && now <= Date.parse(coupon.endDate);
}

// order-service 의 할인 계산과 같게 맞춘다 (정률은 주문 금액의 %, 정액은 그 금액, 주문 금액보다 크지 않게).
export function couponDiscount(coupon: CouponEvent, amount: number): number {
  const discount = coupon.discountType === "PERCENTAGE"
    ? Math.round((amount * (coupon.discountPercentage ?? 0)) / 100)
    : coupon.discountAmount;
  return Math.min(discount, amount);
}

interface IssueVariables {
  promotionId: number;
  onPending?: () => void; // 202 를 받아 폴링을 시작할 때 (카드를 "처리 중"으로 바꾼다)
  signal?: AbortSignal; // 쿠폰 화면을 떠나면 폴링을 멈춘다
}

export function useIssueCoupon() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ promotionId, onPending, signal }: IssueVariables) => issueCoupon(promotionId, onPending, signal),
    // Every outcome can change the remaining count (someone else may have
    // taken the last one), so always refetch the list.
    onSettled: () => queryClient.invalidateQueries({ queryKey: COUPONS_KEY }),
  });
}
