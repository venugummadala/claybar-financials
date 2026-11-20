package com.claybar.events;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** Kafka conventions shared by every Claybar service ({@code claybar.kafka.*}). */
@ConfigurationProperties(prefix = "claybar.kafka")
public class ClaybarKafkaProperties {

    /** Logical service name stamped into {@link EventEnvelope#source()}. Defaults to spring.application.name. */
    private String serviceName;

    /** Create the Claybar topics (and their DLTs) on startup via KafkaAdmin. Disable where topics are managed by IaC. */
    private boolean createTopics = true;

    private int partitions = 3;

    private short replicationFactor = 1;

    private final Retry retry = new Retry();

    public static class Retry {
        /** Delivery attempts after the first failure before the record is dead-lettered. */
        private long maxAttempts = 3;
        /** Initial back-off between attempts (exponential, x2). */
        private Duration initialInterval = Duration.ofSeconds(1);
        private Duration maxInterval = Duration.ofSeconds(10);

        public long getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(long maxAttempts) { this.maxAttempts = maxAttempts; }
        public Duration getInitialInterval() { return initialInterval; }
        public void setInitialInterval(Duration initialInterval) { this.initialInterval = initialInterval; }
        public Duration getMaxInterval() { return maxInterval; }
        public void setMaxInterval(Duration maxInterval) { this.maxInterval = maxInterval; }
    }

    public String getServiceName() { return serviceName; }
    public void setServiceName(String serviceName) { this.serviceName = serviceName; }
    public boolean isCreateTopics() { return createTopics; }
    public void setCreateTopics(boolean createTopics) { this.createTopics = createTopics; }
    public int getPartitions() { return partitions; }
    public void setPartitions(int partitions) { this.partitions = partitions; }
    public short getReplicationFactor() { return replicationFactor; }
    public void setReplicationFactor(short replicationFactor) { this.replicationFactor = replicationFactor; }
    public Retry getRetry() { return retry; }
}
