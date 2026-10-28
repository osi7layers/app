"use client";

import { useTranslations } from "next-intl";
import { AlertCircle, CheckCircle2, Info, Loader2, X } from "lucide-react";
import { Button } from "@ecommerce/ui";
import { cn } from "@ecommerce/lib/utils";
import type { IssueResult } from "@/data/couponMock";
import { couponStyles as s, noticeTone } from "./coupon.styles";

const MESSAGE_KEY: Record<IssueResult, string> = {
  pending: "resultPending",
  success: "resultSuccess",
  failed: "resultFailed",
  delayed: "resultDelayed",
  loginRequired: "resultLoginRequired",
  alreadyClaimed: "resultAlreadyClaimed",
  soldOut: "resultSoldOut",
  notOpen: "resultNotOpen",
  error: "resultError",
};

interface IssueNoticeProps {
  result: IssueResult;
  couponName: string;
  onRetry?: () => void;
  onLogin?: () => void;
  onRecheck?: () => void;
  onDismiss: () => void;
}

// Banner shown above the coupon grid after the issue button is pressed.
export default function IssueNotice({ result, couponName, onRetry, onLogin, onRecheck, onDismiss }: IssueNoticeProps) {
  const t = useTranslations("Coupons");
  const Icon =
    result === "success" ? CheckCircle2
      : result === "pending" ? Loader2
        : result === "error" || result === "failed" ? AlertCircle
          : Info;

  return (
    <div role="status" aria-live="polite" className={cn(s.notice, noticeTone[result])}>
      <Icon className={cn("h-5 w-5 shrink-0", result === "pending" && "animate-spin")} />
      <p className={s.noticeText}>{t(MESSAGE_KEY[result], { name: couponName })}</p>
      {(result === "error" || result === "failed") && onRetry && (
        <Button size="sm" variant="danger" onClick={onRetry}>
          {t("retry")}
        </Button>
      )}
      {result === "delayed" && onRecheck && (
        <Button size="sm" variant="outline" onClick={onRecheck}>
          {t("recheck")}
        </Button>
      )}
      {result === "loginRequired" && onLogin && (
        <Button size="sm" onClick={onLogin}>
          {t("login")}
        </Button>
      )}
      <button type="button" aria-label={t("dismiss")} className={s.noticeClose} onClick={onDismiss}>
        <X className="h-4 w-4" />
      </button>
    </div>
  );
}
