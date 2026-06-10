FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /build

COPY pom.xml ./
RUN --mount=type=cache,target=/root/.m2 mvn -B -DskipTests dependency:go-offline || true

COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -DskipTests clean package


FROM apache/spark:3.5.1

USER root
RUN mkdir -p /opt/app && chmod 0755 /opt/app
COPY --from=builder /build/target/spark-session-metrics-1.0.0-jar-with-dependencies.jar /opt/app/app.jar
RUN chmod 0644 /opt/app/app.jar

ENV HOME=/tmp \
    SPARK_HOME=/opt/spark \
    PATH="/opt/spark/bin:${PATH}"

WORKDIR /opt/app
ENTRYPOINT []
