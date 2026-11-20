package com.claybar.application.outbox;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnProperty(prefix = "claybar.outbox.relay", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxScheduler {

    private final OutboxRelay relay;

    public OutboxScheduler(OutboxRelay relay) {
        this.relay = relay;
    }

    @Scheduled(fixedDelayString = "${claybar.outbox.poll-interval:PT0.5S}")
    public void relay() {
        // Drain quickly under load, yield when idle.
        while (relay.relayBatch() > 0) {
            Thread.onSpinWait();
        }
    }

    @Scheduled(cron = "${claybar.outbox.purge-cron:0 15 3 * * *}")
    public void purge() {
        relay.purgePublished();
    }
}
