# Override for a private registry, e.g. --build-arg BASE_IMAGE=<harbor>/dockerhub-proxy/library/eclipse-temurin:21-jre-alpine
ARG BASE_IMAGE=eclipse-temurin:21-jre-alpine
FROM ${BASE_IMAGE}
# 응답 헤더 X-App-Version에 쓰는 빌드 버전 (Jenkins가 --build-arg APP_VERSION=<git SHA>로 넣음)
ARG APP_VERSION=unknown
ENV APP_VERSION=${APP_VERSION}
COPY target/*.jar app.jar
ENTRYPOINT ["java", "-Duser.timezone=UTC", "-jar", "app.jar"]
