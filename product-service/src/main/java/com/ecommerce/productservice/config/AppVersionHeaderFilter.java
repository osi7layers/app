package com.ecommerce.productservice.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

// 모든 응답에 이 이미지의 빌드 버전(git SHA)을 X-App-Version 헤더로 붙인다.
// Canary 배포 중에 요청마다 stable / canary 중 어느 버전이 응답했는지 curl, k6로 확인하는 용도 (web의 /api/version과 같은 목적).
// APP_VERSION은 Jenkins가 이미지 빌드 때 --build-arg로 넣는다. 로컬 실행처럼 값이 없으면 unknown.
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class AppVersionHeaderFilter extends OncePerRequestFilter {

    private final String appVersion;

    public AppVersionHeaderFilter(@Value("${APP_VERSION:unknown}") String appVersion) {
        this.appVersion = appVersion;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        // 인증 실패(401) 같은 응답에도 붙도록 다음 필터로 넘기기 전에 넣는다
        response.setHeader("X-App-Version", appVersion);
        chain.doFilter(request, response);
    }
}
