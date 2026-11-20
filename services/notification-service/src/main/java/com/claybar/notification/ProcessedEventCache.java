package com.claybar.notification;

import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Bounded LRU of handled event ids: suppresses duplicate notifications on Kafka redelivery.
 * Per-instance and in-memory by design (a duplicate e-mail after a restart is acceptable here);
 * use Redis or a DB table where exactly-once delivery to the customer is a hard requirement.
 */
@Component
public class ProcessedEventCache {

    private final Set<String> seen;

    public ProcessedEventCache(NotificationProperties props) {
        int max = props.dedupeCacheSize();
        this.seen = Collections.synchronizedSet(Collections.newSetFromMap(new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > max;
            }
        }));
    }

    /** @return true if the id was not seen before (and is now recorded) */
    public boolean markIfNew(String eventId) {
        return seen.add(eventId);
    }
}
