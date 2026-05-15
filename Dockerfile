# Multi-target image: build with `docker build --target ordering-api` (or kitchen-service / runner-service).
FROM eclipse-temurin:17-jdk AS builder
WORKDIR /workspace
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle ./gradle
COPY common ./common
COPY ordering-api ./ordering-api
COPY kitchen-service ./kitchen-service
COPY runner-service ./runner-service
RUN chmod +x gradlew \
    && ./gradlew :ordering-api:installDist :kitchen-service:installDist :runner-service:installDist --no-daemon

FROM eclipse-temurin:17-jre AS ordering-api
WORKDIR /app
COPY --from=builder /workspace/ordering-api/build/install/ordering-api ./
EXPOSE 8080
ENTRYPOINT ["/app/bin/ordering-api"]

FROM eclipse-temurin:17-jre AS kitchen-service
WORKDIR /app
COPY --from=builder /workspace/kitchen-service/build/install/kitchen-service ./
ENTRYPOINT ["/app/bin/kitchen-service"]

FROM eclipse-temurin:17-jre AS runner-service
WORKDIR /app
COPY --from=builder /workspace/runner-service/build/install/runner-service ./
ENTRYPOINT ["/app/bin/runner-service"]
