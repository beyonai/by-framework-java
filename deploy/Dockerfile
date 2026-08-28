# Deploy smoke test image: builds by-framework and runs the DemoWorker example
# worker (see by-framework/src/main/java/.../examples/DemoWorker.java) against a
# real Redis instance. Driven by deploy/docker-compose.yml and
# .github/workflows/deploy-smoke-test.yml — see docs there for what this actually
# verifies end to end.

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

# Copy poms first so dependency resolution is cached across source-only changes.
COPY pom.xml .
COPY by-framework/pom.xml by-framework/pom.xml
COPY by-framework-history/pom.xml by-framework-history/pom.xml
COPY by-framework-history/by-framework-history-postgres/pom.xml by-framework-history/by-framework-history-postgres/pom.xml
COPY by-framework-history/by-framework-history-byclaw/pom.xml by-framework-history/by-framework-history-byclaw/pom.xml
COPY config config

COPY by-framework/src by-framework/src
# logback-classic is test-scoped in by-framework/pom.xml on purpose (a library
# shouldn't force a logging backend on consumers) - fetch a minimal SLF4J
# binding here instead, just for this runnable deploy image, so the worker's
# LOG.info/error calls actually reach `docker compose logs worker` instead of
# silently no-op-ing on the NOP logger.
RUN mvn -B -pl by-framework -am package -DskipTests \
    && mvn -B -pl by-framework dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=/workspace/by-framework/target/dependency \
    && mvn -B dependency:copy -Dartifact=org.slf4j:slf4j-simple:2.0.7 -DoutputDirectory=/workspace/by-framework/target/dependency

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/by-framework/target/by-framework-*.jar app.jar
COPY --from=build /workspace/by-framework/target/dependency dependency

# Overridable so the same image can also run SendAndVerify (see docker-compose.yml).
ENV WORKER_CLASS=com.iwhaleai.byai.framework.examples.DemoWorker
ENTRYPOINT ["sh", "-c", "java -cp app.jar:dependency/* \"$WORKER_CLASS\""]
