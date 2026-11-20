package com.claybar.credit;

import com.claybar.events.EventCodec;
import com.claybar.events.EventEnvelope;
import com.claybar.events.EventTypes;
import com.claybar.events.Topics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end through a real (embedded) Kafka broker: command in, event out, poison message to DLT.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "claybar.kafka.retry.max-attempts=1",
        "claybar.kafka.retry.initial-interval=50ms",
})
@EmbeddedKafka(partitions = 1, kraft = true)
class CreditServiceKafkaIntegrationTest {

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired EventCodec codec;
    @Autowired EmbeddedKafkaBroker broker;

    @Test
    void repliesToCreditCheckRequestedWithScore() {
        EventEnvelope command = EventEnvelope.of(EventTypes.CREDIT_CHECK_REQUESTED, "APP-IT-1", "test",
                Map.of("nationalId", "PRIME-42", "annualIncome", 90000, "loanAmount", 18000));

        try (Consumer<String, String> consumer = consumer("it-replies", Topics.CREDIT_EVENTS)) {
            kafka.send(Topics.CREDIT_COMMANDS, command.correlationId(), codec.encode(command));

            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, Topics.CREDIT_EVENTS, Duration.ofSeconds(20));
            EventEnvelope reply = codec.decode(record.value());

            assertThat(record.key()).isEqualTo("APP-IT-1");
            assertThat(reply.eventType()).isEqualTo(EventTypes.CREDIT_CHECK_COMPLETED);
            assertThat(reply.causationId()).isEqualTo(command.eventId());
            assertThat(reply.payload()).containsEntry("creditScore", 800).containsEntry("riskBand", "PRIME")
                    .containsEntry("debtToIncome", 0.2);
        }
    }

    @Test
    void poisonMessageIsDeadLettered() {
        try (Consumer<String, String> consumer = consumer("it-dlt", Topics.CREDIT_COMMANDS + Topics.DLT_SUFFIX)) {
            kafka.send(Topics.CREDIT_COMMANDS, "APP-POISON", "{this is not json");

            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer,
                    Topics.CREDIT_COMMANDS + Topics.DLT_SUFFIX, Duration.ofSeconds(20));
            assertThat(record.value()).isEqualTo("{this is not json");
            assertThat(record.headers().lastHeader("kafka_dlt-exception-fqcn")).isNotNull();
        }
    }

    private Consumer<String, String> consumer(String group, String topic) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(group, "true", broker);
        props.put("auto.offset.reset", "earliest");
        Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                new StringDeserializer()).createConsumer();
        consumer.subscribe(java.util.List.of(topic));
        return consumer;
    }
}
