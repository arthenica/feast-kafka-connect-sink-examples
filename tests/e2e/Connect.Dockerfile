FROM apache/kafka:4.3.0 AS kafka
FROM eclipse-temurin:21-jdk AS compile
COPY --from=kafka /opt/kafka/libs/ /kafka-libs/
COPY KafkaSourceId.java /src/KafkaSourceId.java
RUN javac -proc:none -cp '/kafka-libs/*' -d /classes /src/KafkaSourceId.java \
    && jar --create --file /kafka-source-id-smt.jar -C /classes .

FROM apache/kafka:4.3.0
COPY worker.properties /config/worker.properties
COPY plugin/ /opt/connect-plugins/
COPY --from=compile /kafka-source-id-smt.jar /opt/connect-plugins/kafka-source-id-smt/kafka-source-id-smt.jar
ENTRYPOINT ["/opt/kafka/bin/connect-distributed.sh", "/config/worker.properties"]
