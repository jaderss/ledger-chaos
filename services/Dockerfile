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
RUN ./mvnw -q -B -pl ${MODULE} dependency:go-offline
COPY ${MODULE}/src ${MODULE}/src
RUN ./mvnw -q -B -pl ${MODULE} package -DskipTests -Dspotless.check.skip=true -Djacoco.skip=true \
    && cp ${MODULE}/target/${MODULE}-*.jar /app.jar

FROM eclipse-temurin:${JAVA_VERSION}-jre
COPY --from=build /app.jar /app/app.jar
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
