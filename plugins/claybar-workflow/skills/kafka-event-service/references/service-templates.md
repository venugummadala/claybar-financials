# Service templates

Replace `fraud` / `Fraud` / port `8085`. Package `com.claybar.<name>`.

## Contents
pom.xml · Application · Properties · Domain port + logic · Listener · application.yml · Unit test ·
Embedded-Kafka test · docker-compose entry

## services/fraud-service/pom.xml

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>com.claybar</groupId>
        <artifactId>claybar-financials</artifactId>
        <version>1.0.0-SNAPSHOT</version>
        <relativePath>../../pom.xml</relativePath>
    </parent>
    <artifactId>fraud-service</artifactId>
    <name>Claybar Financials :: Fraud Service</name>

    <dependencies>
        <dependency><groupId>com.claybar</groupId><artifactId>claybar-events</artifactId></dependency>
        <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-web</artifactId></dependency>
        <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-actuator</artifactId></dependency>
        <dependency><groupId>io.micrometer</groupId><artifactId>micrometer-registry-prometheus</artifactId></dependency>
        <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-test</artifactId><scope>test</scope></dependency>
        <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka-test</artifactId><scope>test</scope></dependency>
    </dependencies>
    <build>
        <plugins>
            <plugin><groupId>org.springframework.boot</groupId><artifactId>spring-boot-maven-plugin</artifactId></plugin>
        </plugins>
    </build>
</project>
```

## Application and properties

```java
@SpringBootApplication
@ConfigurationPropertiesScan
public class FraudServiceApplication {
    public static void main(String[] args) { SpringApplication.run(FraudServiceApplication.class, args); }
}

@ConfigurationProperties(prefix = "claybar.fraud")
public record FraudProperties(int highRiskThreshold) {
    public FraudProperties { highRiskThreshold = highRiskThreshold <= 0 ? 80 : highRiskThreshold; }
}
```

## Domain port and logic

```java
/** Port to the external fraud provider. */
public interface FraudScoringClient {
    FraudAssessment assess(String nationalId, double loanAmount);
}

public record FraudAssessment(int fraudScore, String reason) { }

/** Deterministic simulator: same input, same score — so redelivery is harmless. */
@Component
public class SimulatedFraudScoringClient implements FraudScoringClient {
    @Override
    public FraudAssessment assess(String nationalId, double loanAmount) {
        CRC32 crc = new CRC32();
        crc.update(nationalId.toUpperCase().getBytes(StandardCharsets.UTF_8));
        int score = (int) (crc.getValue() % 101);
        return new FraudAssessment(score, score >= 80 ? "Velocity / device mismatch" : "No indicators");
    }
}
```

## Listener

```java
@Component
public class FraudCommandListener {

    private static final Logger log = LoggerFactory.getLogger(FraudCommandListener.class);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    private final EventCodec codec;
    private final EventPublisher publisher;
    private final FraudScoringClient client;
    private final MeterRegistry meters;

    public FraudCommandListener(EventCodec codec, EventPublisher publisher, FraudScoringClient client, MeterRegistry meters) {
        this.codec = codec;
        this.publisher = publisher;
        this.client = client;
        this.meters = meters;
    }

    @KafkaListener(topics = Topics.FRAUD_COMMANDS)
    public void onCommand(ConsumerRecord<String, String> record) {
        EventEnvelope command = codec.decode(record.value());                 // poison -> DLT
        if (!EventTypes.FRAUD_CHECK_REQUESTED.equals(command.eventType())) {
            return;                                                           // not ours
        }
        try (var ignored = CorrelationContext.open(command)) {
            String nationalId = command.payloadString("nationalId");
            if (nationalId == null || nationalId.isBlank()) {
                throw new NonRetryableEventException("FraudCheckRequested without nationalId for " + command.correlationId());
            }
            Number amount = command.payloadNumber("loanAmount");
            FraudAssessment result = client.assess(nationalId, amount == null ? 0d : amount.doubleValue());

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("fraudStatus", "COMPLETED");
            payload.put("fraudScore", result.fraudScore());
            payload.put("fraudReason", result.reason());

            publisher.publishAndWait(Topics.FRAUD_EVENTS,
                    EventEnvelope.causedBy(command, EventTypes.FRAUD_CHECK_COMPLETED, "fraud-service", payload), SEND_TIMEOUT);
            meters.counter("claybar.fraud.checks").increment();
            log.info("Fraud check for {} -> {}", command.correlationId(), result.fraudScore());
        }
    }
}
```

## src/main/resources/application.yml

```yaml
server:
  port: 8085
  shutdown: graceful
spring:
  application:
    name: fraud-service
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    consumer:
      group-id: fraud-service
      auto-offset-reset: earliest
    producer:
      acks: all
      properties:
        enable.idempotence: true
    listener:
      concurrency: 3
claybar:
  kafka:
    service-name: fraud-service
  fraud:
    high-risk-threshold: 80
management:
  endpoints.web.exposure.include: health,info,prometheus,metrics
  endpoint.health.probes.enabled: true
  metrics.tags.application: ${spring.application.name}
logging:
  pattern:
    level: "%5p [${spring.application.name},%X{correlationId:-}]"
```
Durations used by `@Scheduled(fixedDelayString=…)` must be ISO-8601 (`PT0.5S`) — `500ms` fails at startup.

## Unit test

```java
class SimulatedFraudScoringClientTest {
    private final SimulatedFraudScoringClient client = new SimulatedFraudScoringClient();

    @Test
    void deterministicAndInRange() {
        int first = client.assess("AB123", 1000).fraudScore();
        assertThat(client.assess("ab123", 1000).fraudScore()).isEqualTo(first);
        assertThat(first).isBetween(0, 100);
    }
}
```

## Embedded-Kafka integration test

```java
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "claybar.kafka.retry.max-attempts=1",
        "claybar.kafka.retry.initial-interval=50ms",
})
@EmbeddedKafka(partitions = 1, kraft = true)
class FraudServiceKafkaIntegrationTest {
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired EventCodec codec;
    @Autowired EmbeddedKafkaBroker broker;

    @Test
    void repliesWithScore() {
        EventEnvelope command = EventEnvelope.of(EventTypes.FRAUD_CHECK_REQUESTED, "APP-IT-1", "test",
                Map.of("nationalId", "AB123", "loanAmount", 20000));
        try (Consumer<String, String> consumer = consumer("it-replies", Topics.FRAUD_EVENTS)) {
            kafka.send(Topics.FRAUD_COMMANDS, command.correlationId(), codec.encode(command));
            var record = KafkaTestUtils.getSingleRecord(consumer, Topics.FRAUD_EVENTS, Duration.ofSeconds(20));
            EventEnvelope reply = codec.decode(record.value());
            assertThat(record.key()).isEqualTo("APP-IT-1");
            assertThat(reply.eventType()).isEqualTo(EventTypes.FRAUD_CHECK_COMPLETED);
            assertThat(reply.causationId()).isEqualTo(command.eventId());
            assertThat(reply.payload()).containsKeys("fraudScore", "fraudStatus");
        }
    }

    @Test
    void poisonMessageIsDeadLettered() {
        String dlt = Topics.FRAUD_COMMANDS + Topics.DLT_SUFFIX;
        try (Consumer<String, String> consumer = consumer("it-dlt", dlt)) {
            kafka.send(Topics.FRAUD_COMMANDS, "APP-POISON", "{not json");
            assertThat(KafkaTestUtils.getSingleRecord(consumer, dlt, Duration.ofSeconds(20)).value()).isEqualTo("{not json");
        }
    }

    private Consumer<String, String> consumer(String group, String topic) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(group, "true", broker);
        props.put("auto.offset.reset", "earliest");
        Consumer<String, String> c = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer()).createConsumer();
        c.subscribe(List.of(topic));
        return c;
    }
}
```

## docker-compose.yml entry

```yaml
  fraud-service:
    <<: *service
    build: { context: ., args: { MODULE: services/fraud-service } }
    image: claybar/fraud-service:local
    ports: ["8085:8085"]
    environment:
      <<: *service-env
      SERVER_PORT: 8085
```
