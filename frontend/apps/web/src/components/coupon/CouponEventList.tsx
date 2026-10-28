"use client";

import { useEffect, useRef, useState } from "react";
import { useTranslations } from "next-intl";
import { TicketX } from "lucide-react";
import { EmptyState, LoadingSkeleton } from "@ecommerce/ui";
import { useAuthStore } from "@ecommerce/lib/store";
import { useRouter } from "@/i18n/navigation";
import { COUPON_MOCK_MODE, useCoupons, useIssueCoupon } from "@/hooks";
import { getCouponStatus, type CouponEvent, type IssueResult } from "@/data/couponMock";
import CouponHero from "./CouponHero";
import CouponCard from "./CouponCard";
import IssueNotice from "./IssueNotice";
import { couponStyles as s } from "./coupon.styles";

interface Notice {
  result: IssueResult;
  coupon: CouponEvent;
}

export default function CouponEventList() {
  const t = useTranslations("Coupons");
  const router = useRouter();
  const { isAuthenticated } = useAuthStore();
  const { data: coupons, isLoading, isError } = useCoupons();
  const issue = useIssueCoupon();
  const [notice, setNotice] = useState<Notice | null>(null);
  // 202 를 받아 결과를 기다리는 쿠폰. 그 카드만 "처리 중"으로 보여 준다.
  const [pendingId, setPendingId] = useState<number | null>(null);

  // 쿠폰 화면을 떠나면 진행 중인 상태 조회를 멈춘다.
  const abortRef = useRef<AbortController | null>(null);
  useEffect(() => () => abortRef.current?.abort(), []);

  // Ticking clock for countdowns and open/close transitions. It starts as null
  // and is set after mount so server and client render the same markup.
  const [now, setNow] = useState<number | null>(null);
  useEffect(() => {
    const tick = () => setNow(Date.now());
    tick();
    const timer = setInterval(tick, 1000);
    return () => clearInterval(timer);
  }, []);

  const handleIssue = (coupon: CouponEvent) => {
    if (!COUPON_MOCK_MODE && !isAuthenticated) {
      router.push("/login?redirect=/coupons");
      return;
    }
    setNotice(null);
    setPendingId(null);
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    const finish = (result: IssueResult) => {
      if (controller.signal.aborted) return;
      setPendingId(null);
      setNotice({ result, coupon });
    };
    issue.mutate(
      {
        promotionId: coupon.promotionId,
        onPending: () => {
          setPendingId(coupon.promotionId);
          setNotice({ result: "pending", coupon });
        },
        signal: controller.signal,
      },
      {
        onSuccess: finish,
        onError: () => finish("error"),
      },
    );
  };

  const nextOpenAt =
    now === null || !coupons
      ? null
      : coupons
          .map((c) => Date.parse(c.startDate))
          .filter((start) => start > now)
          .sort((a, b) => a - b)[0] ?? null;

  return (
    <div className="max-w-5xl mx-auto px-4 py-6 space-y-4">
      <CouponHero nextOpenAt={nextOpenAt} now={now} />

      {notice && (
        <IssueNotice
          result={notice.result}
          couponName={notice.coupon.name}
          onRetry={() => handleIssue(notice.coupon)}
          onLogin={() => router.push("/login?redirect=/coupons")}
          onDismiss={() => setNotice(null)}
        />
      )}

      {isLoading || now === null ? (
        <LoadingSkeleton rows={4} />
      ) : isError ? (
        <EmptyState icon={<TicketX className="h-20 w-20 text-gray-200" />} title={t("loadError")} />
      ) : !coupons || coupons.length === 0 ? (
        <EmptyState
          icon={<TicketX className="h-20 w-20 text-gray-200" />}
          title={t("emptyTitle")}
          description={t("emptyHint")}
        />
      ) : (
        <div className={coupons.length === 1 ? s.gridSingle : s.grid}>
          {coupons.map((coupon) => (
            <CouponCard
              key={coupon.promotionId}
              coupon={coupon}
              status={pendingId === coupon.promotionId ? "pending" : getCouponStatus(coupon, now)}
              now={now}
              issuing={issue.isPending && issue.variables?.promotionId === coupon.promotionId}
              onIssue={handleIssue}
              featured={coupons.length === 1}
            />
          ))}
        </div>
      )}
    </div>
  );
}
