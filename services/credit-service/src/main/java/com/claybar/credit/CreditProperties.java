package com.claybar.credit;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "claybar.credit")
public record CreditProperties(Duration simulatedLatency) {

    public CreditProperties {
        simulatedLatency = simulatedLatency == null ? Duration.ZERO : simulatedLatency;
    }
}
