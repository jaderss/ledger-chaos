ARG JAVA_VERSION=25

FROM eclipse-temurin:${JAVA_VERSION}-jdk AS build
WORKDIR /src
COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY ledger/pom.xml ledger/
RUN ./mvnw -q -B -pl ledger dependency:go-offline
COPY ledger/src ledger/src
RUN ./mvnw -q -B -pl ledger package -DskipTests -Dspotless.check.skip=true -Djacoco.skip=true \
    && cp ledger/target/ledger-*.jar /app.jar

FROM eclipse-temurin:${JAVA_VERSION}-jre
COPY --from=build /app.jar /app/app.jar
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
