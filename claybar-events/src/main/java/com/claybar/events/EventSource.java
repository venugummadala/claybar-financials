package com.claybar.events;

import org.springframework.core.env.Environment;

/** Resolves the logical producer name used in {@link EventEnvelope#source()}. */
public final class EventSource {

    private EventSource() {
    }

    public static String resolve(ClaybarKafkaProperties props, Environment env) {
        if (props.getServiceName() != null && !props.getServiceName().isBlank()) {
            return props.getServiceName();
        }
        return env.getProperty("spring.application.name", "claybar-service");
    }
}
