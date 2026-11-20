# syntax=docker/dockerfile:1.7
# One image recipe for every service. Build the jars first (mvn -DskipTests package), then:
#   docker build --build-arg MODULE=services/kyc-service -t claybar/kyc-service .
# Uses Spring Boot layered jars so dependency layers are cached across releases.

FROM eclipse-temurin:21-jre-alpine AS extract
ARG MODULE
WORKDIR /build
COPY ${MODULE}/target/*.jar app.jar
RUN java -Djarmode=layertools -jar app.jar extract

FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S claybar && adduser -S claybar -G claybar
WORKDIR /app
COPY --from=extract /build/dependencies/ ./
COPY --from=extract /build/spring-boot-loader/ ./
COPY --from=extract /build/snapshot-dependencies/ ./
COPY --from=extract /build/application/ ./
USER claybar
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError -Djava.security.egd=file:/dev/./urandom"
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
