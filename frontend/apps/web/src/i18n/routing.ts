import { defineRouting } from "next-intl/routing";

// web은 한국어(ko)를 기본으로 하고 vi, en도 고를 수 있게 둔다.
// admin은 messages에 ko가 없어서 공용 @ecommerce/lib/i18n(vi, en)을 그대로 쓴다.
export const routing = defineRouting({
  locales: ["ko", "vi", "en"],
  defaultLocale: "ko",
  localePrefix: "as-needed",
  // 브라우저 언어나 예전에 고른 언어(NEXT_LOCALE 쿠키) 대신 주소로만 언어를 정한다.
  // 그래서 예전에 vi로 들어왔던 사람도 / 로 들어오면 한국어 화면을 본다.
  localeDetection: false,
});
