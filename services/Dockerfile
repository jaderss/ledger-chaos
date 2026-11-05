ARG JAVA_VERSION=25

FROM eclipse-temurin:${JAVA_VERSION}-jdk AS build
ARG MODULE
WORKDIR /src
COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY ledger/pom.xml ledger/
COPY relay/pom.xml relay/
COPY projector/pom.xml projector/
COPY checker/pom.xml checker/
COPY chaos/pom.xml chaos/
RUN ./mvnw -q -B -pl ${MODULE} dependency:go-offline
COPY ${MODULE}/src ${MODULE}/src
RUN ./mvnw -q -B -pl ${MODULE} package -DskipTests -Dspotless.check.skip=true -Djacoco.skip=true \
    && cp ${MODULE}/target/${MODULE}-*.jar /app.jar

FROM eclipse-temurin:${JAVA_VERSION}-jdk AS tool-runtime
RUN jlink --add-modules java.base,java.desktop,java.logging,java.management,java.naming,java.net.http,java.security.jgss,java.security.sasl,java.sql,jdk.jfr,jdk.unsupported \
    --strip-debug --no-header-files --no-man-pages --compress=zip-9 --output /runtime

FROM gcr.io/distroless/cc-debian13:nonroot AS tool
COPY --from=tool-runtime /runtime /opt/java
COPY --from=build /app.jar /app/app.jar
ENTRYPOINT ["/opt/java/bin/java", "-jar", "/app/app.jar"]

FROM eclipse-temurin:${JAVA_VERSION}-jre AS service
COPY --from=build /app.jar /app/app.jar
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
