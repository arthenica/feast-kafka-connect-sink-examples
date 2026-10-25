import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/** Real Connect worker regression: a preCommit rejection must wait for Kafka's DLQ ack. */
public class DlqCommitRegression {
    private static final String CONNECTOR = "dlq-commit-regression";
    private static final String INPUT = "dlq-commit-input";
    private static final String DLQ = "dlq-commit-errors";
    private static final TopicPartition INPUT_PARTITION = new TopicPartition(INPUT, 0);
    private static final TopicPartition DLQ_PARTITION = new TopicPartition(DLQ, 0);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(3)).build();

    public static void main(String[] args) {
        try {
            run();
            System.out.println("PASS: offsets stayed uncommitted while the DLQ write was blocked; "
                + "the record reached the DLQ and offset 1 committed after release.");
        } catch (Throwable failure) {
            System.err.println("FAIL: " + failure.getMessage());
            failure.printStackTrace();
            System.exit(1);
        }
    }

    private static void run() throws Exception {
        var rejected = new CountDownLatch(1);
        var feast = HttpServer.create(new InetSocketAddress(8080), 0);
        feast.createContext("/push", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            rejected.countDown();
        });
        feast.start();

        try (var gate = new ProduceGate();
             var admin = Admin.create(Map.of("bootstrap.servers", "kafka:9092"));
             var consumer = new KafkaConsumer<String, String>(
                 Map.of("bootstrap.servers", "kafka:9092", "enable.auto.commit", false),
                 new StringDeserializer(), new StringDeserializer())) {
            await("Connect REST API", Duration.ofSeconds(120), () -> {
                try { return request("GET", "/connectors", null).statusCode() == 200; }
                catch (Exception unavailable) { return false; }
            });
            admin.createTopics(List.of(new NewTopic(INPUT, 1, (short) 1),
                new NewTopic(DLQ, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);

            var config = Map.ofEntries(
                Map.entry("connector.class", "com.arthenica.kafka.feastconnect.sink.connector.FeastSinkConnector"),
                Map.entry("tasks.max", "1"), Map.entry("topics", INPUT),
                Map.entry("feast.url", "http://test:8080"),
                Map.entry("feast.push.source.name", "regression_push_source"),
                Map.entry("entity.key.fields", "entity_id"),
                // One row must remain buffered until preCommit, never size/linger flushing.
                Map.entry("batch.size", "500"), Map.entry("linger.ms", "86400000"),
                Map.entry("errors.tolerance", "all"),
                Map.entry("errors.deadletterqueue.topic.name", DLQ),
                Map.entry("errors.deadletterqueue.topic.replication.factor", "1"),
                Map.entry("errors.deadletterqueue.context.headers.enable", "true"),
                // Only the DLQ producer goes through the gate. Offset commits go directly to Kafka.
                Map.entry("producer.override.bootstrap.servers", "test:19094"),
                Map.entry("producer.override.acks", "all"),
                Map.entry("producer.override.request.timeout.ms", "60000"),
                Map.entry("producer.override.delivery.timeout.ms", "120000"));
            var registration = request("POST", "/connectors",
                JSON.writeValueAsString(Map.of("name", CONNECTOR, "config", config)));
            check(registration.statusCode() == 201, "Connector registration failed: " + registration.body());
            await("running sink task", Duration.ofSeconds(60), DlqCommitRegression::taskRunning);

            String event = "{\"entity_id\":\"dlq-regression-1\","
                + "\"event_timestamp\":\"2024-01-01T00:00:00Z\",\"feature\":1}";
            try (var producer = new KafkaProducer<String, String>(
                Map.of("bootstrap.servers", "kafka:9092", "acks", "all"),
                new StringSerializer(), new StringSerializer())) {
                var sent = producer.send(new ProducerRecord<>(INPUT, 0, "test", event))
                    .get(30, TimeUnit.SECONDS);
                check(sent.offset() == 0, "Input topic must start empty");
            }

            check(gate.blocked.await(30, TimeUnit.SECONDS), "No DLQ Produce request reached the gate");
            check(rejected.getCount() == 0, "The HTTP stub did not reject the preCommit flush");
            consumer.assign(List.of(DLQ_PARTITION));
            consumer.seekToBeginning(List.of(DLQ_PARTITION));

            // Observe multiple commit intervals after an actual Produce request was intercepted.
            long deadline = System.nanoTime() + Duration.ofSeconds(6).toNanos();
            do {
                gate.checkHealthy();
                check(taskRunning(), "Sink task stopped while the DLQ was blocked");
                long committed = committedOffset(admin);
                check(committed < 1, "Input offset advanced to " + committed
                    + " before the rejected record was written to the DLQ");
                check(consumer.endOffsets(List.of(DLQ_PARTITION), Duration.ofSeconds(5))
                    .get(DLQ_PARTITION) == 0, "DLQ was populated while Produce requests were blocked");
                Thread.sleep(100);
            } while (System.nanoTime() < deadline);
            System.out.println("Confirmed: no input offset committed during six seconds of blocked DLQ writes.");

            gate.release.countDown();
            await("source offset 1 to commit after DLQ acknowledgement", Duration.ofSeconds(30), () -> {
                try { return committedOffset(admin) == 1; }
                catch (Exception failure) { throw new RuntimeException(failure); }
            });
            var records = consumer.poll(Duration.ofSeconds(10));
            check(records.count() == 1, "Expected one durable DLQ record, got " + records.count());
            var record = records.iterator().next();
            check(event.equals(record.value()), "DLQ payload differs from the rejected input");
            check(record.headers().lastHeader("__connect.errors.topic") != null,
                "Missing Kafka Connect DLQ context headers");
        } finally {
            feast.stop(0);
        }
    }

    private static long committedOffset(Admin admin) throws Exception {
        var offsets = admin.listConsumerGroupOffsets("connect-" + CONNECTOR)
            .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
        var offset = offsets.get(INPUT_PARTITION);
        return offset == null ? -1 : offset.offset();
    }

    private static boolean taskRunning() {
        try {
            JsonNode status = JSON.readTree(request("GET", "/connectors/" + CONNECTOR + "/status", null).body());
            if (status.path("tasks").isEmpty()) return false;
            for (var task : status.path("tasks")) {
                check(!task.path("state").asText().equals("FAILED"), "Sink task failed: " + status);
                if (!task.path("state").asText().equals("RUNNING")) return false;
            }
            return true;
        } catch (IOException | InterruptedException unavailable) {
            return false;
        }
    }

    private static HttpResponse<String> request(String method, String path, String body)
        throws IOException, InterruptedException {
        return HTTP.send(HttpRequest.newBuilder(java.net.URI.create("http://connect:8083" + path))
            .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
            .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private static void await(String description, Duration timeout, BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            check(System.nanoTime() < deadline, "Timed out waiting for " + description);
            Thread.sleep(200);
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    /** Frames Kafka requests, holding ApiKey 0 (Produce) before forwarding it to the broker. */
    private static final class ProduceGate implements AutoCloseable {
        final CountDownLatch blocked = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        private final ServerSocket listener = new ServerSocket(19094);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private volatile boolean closed;

        ProduceGate() throws IOException {
            Thread.startVirtualThread(() -> {
                try {
                    while (!closed) {
                        Socket client = listener.accept();
                        Thread.startVirtualThread(() -> forward(client));
                    }
                } catch (IOException error) {
                    if (!closed) failure.compareAndSet(null, error);
                }
            });
        }

        private void forward(Socket client) {
            try (client; var broker = new Socket("kafka", 9094)) {
                Thread.startVirtualThread(() -> {
                    try { broker.getInputStream().transferTo(client.getOutputStream()); }
                    catch (IOException ignored) { /* The client may close a bootstrap connection. */ }
                });
                var input = new DataInputStream(client.getInputStream());
                var output = new DataOutputStream(broker.getOutputStream());
                while (true) {
                    int size;
                    try { size = input.readInt(); } catch (EOFException done) { return; }
                    check(size >= 2 && size <= 10 * 1024 * 1024, "Invalid Kafka request length: " + size);
                    byte[] frame = input.readNBytes(size);
                    if (frame.length != size) throw new EOFException("Truncated Kafka request");
                    if (ByteBuffer.wrap(frame).getShort() == 0 && release.getCount() != 0) {
                        blocked.countDown();
                        check(release.await(90, TimeUnit.SECONDS), "Timed out holding DLQ Produce request");
                    }
                    output.writeInt(size);
                    output.write(frame);
                    output.flush();
                }
            } catch (Exception | AssertionError error) {
                if (!closed) failure.compareAndSet(null, error);
            }
        }

        void checkHealthy() {
            check(failure.get() == null, "DLQ gate failed: " + failure.get());
        }

        @Override public void close() throws IOException {
            closed = true;
            release.countDown();
            listener.close();
        }
    }
}
