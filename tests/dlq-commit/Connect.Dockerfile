FROM apache/kafka:4.3.0
COPY worker.properties /config/worker.properties
COPY plugin/ /opt/connect-plugins/
ENTRYPOINT ["/opt/kafka/bin/connect-distributed.sh", "/config/worker.properties"]
