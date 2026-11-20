package com.claybar.application.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "claybar.outbox")
public record OutboxProperties(Duration pollInterval, int batchSize, Duration sendTimeout, Duration retention) {

    public OutboxProperties {
        pollInterval = pollInterval == null ? Duration.ofMillis(500) : pollInterval;
        batchSize = batchSize <= 0 ? 100 : batchSize;
        sendTimeout = sendTimeout == null ? Duration.ofSeconds(10) : sendTimeout;
        retention = retention == null ? Duration.ofDays(7) : retention;
    }
}
