package com.claybar.notification;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "claybar.notification")
public record NotificationProperties(String opsRecipient, int dedupeCacheSize) {

    public NotificationProperties {
        opsRecipient = opsRecipient == null ? "loan-ops@claybar.example" : opsRecipient;
        dedupeCacheSize = dedupeCacheSize <= 0 ? 10_000 : dedupeCacheSize;
    }
}
