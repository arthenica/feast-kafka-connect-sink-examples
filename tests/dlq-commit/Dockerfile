FROM apache/kafka:4.3.0 AS kafka
FROM eclipse-temurin:21-jdk AS compile
COPY --from=kafka /opt/kafka/libs/ /kafka-libs/
WORKDIR /src
COPY DlqCommitRegression.java ./
RUN javac -proc:none -cp '/kafka-libs/*' -d /classes DlqCommitRegression.java

FROM apache/kafka:4.3.0
COPY --from=compile /classes/ /tests/
ENTRYPOINT ["java", "-cp", "/tests:/opt/kafka/libs/*", "DlqCommitRegression"]
