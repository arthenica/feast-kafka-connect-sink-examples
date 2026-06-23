# Kafka Connect worker (Apache Kafka distribution) with the feast-kafka-connect-sink plugin.
#
# The plugin is supplied by unzipping the connector's distribution ZIP into ./plugin/ before
# building (see README.md), so this image does not depend on the connector's Gradle build:
#
#   unzip /path/to/feast-kafka-connect-sink-<version>.zip -d plugin/
#   # -> plugin/feast-kafka-connect-sink-<version>/lib/*.jar
#
# docker-compose.yml builds this with the repo root as the context.
FROM apache/kafka:4.3.0

# plugin.path in connect-distributed.properties points here.
COPY plugin/ /opt/connect-plugins/
