# Deploy smoke test image: runs examples/DemoWorker (a plain echo worker) against
# a real Redis instance via `mvn exec:java` - no separate jar-packaging step
# needed. Driven by deploy/docker-compose.yml and
# .github/workflows/deploy-smoke-test.yml.
#
# Only the by-framework module (plus the root aggregator pom.xml and its
# config/checkstyle/checkstyle.xml, which the inherited checkstyle execution
# needs at compile time) is copied in - by-framework-history is a sibling
# module DemoWorker doesn't depend on, so it's left out of the build context
# entirely.

FROM maven:3.9-eclipse-temurin-21
WORKDIR /app

COPY pom.xml ./
COPY config config
COPY by-framework/pom.xml by-framework/pom.xml
COPY by-framework/src by-framework/src

WORKDIR /app/by-framework

# Compile once at build time so the runtime `exec:java` below only has to
# load already-built classes. Also warms the local Maven repo with the
# exec-maven-plugin jar (not declared in pom.xml, invoked ad hoc by full
# coordinates below) so the runtime invocation can go fully offline (-o) -
# the plugin resolves during this dry run even though DemoWorker's main()
# fails immediately with no Redis reachable at build time.
RUN mvn -B -q compile \
    && (mvn -B -q org.codehaus.mojo:exec-maven-plugin:3.1.0:java \
        -Dexec.mainClass=com.iwhaleai.byai.framework.examples.DemoWorker \
        -Dexec.classpathScope=compile || true)

# Overridable so the same image can also run a different examples.* main
# class if needed.
ENV WORKER_CLASS=com.iwhaleai.byai.framework.examples.DemoWorker
# GatewayConfig falls back to the myuser/mypassword placeholder baked into
# gateway-config.properties (a config template, not a real default) whenever
# REDIS_USERNAME/REDIS_PASSWORD aren't set - override both to empty so the
# unauthenticated Redis service below actually connects instead of failing
# with WRONGPASS.
ENV REDIS_USERNAME=""
ENV REDIS_PASSWORD=""
ENTRYPOINT ["sh", "-c", "mvn -o -q org.codehaus.mojo:exec-maven-plugin:3.1.0:java -Dexec.mainClass=\"$WORKER_CLASS\" -Dexec.classpathScope=compile"]
