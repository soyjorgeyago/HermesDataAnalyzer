package es.us.lsi.hermes.kafka;

import es.us.lsi.hermes.kafka.Event;
import java.util.Map;

public class ExtendedEvent extends Event {

    private int retries;

    public ExtendedEvent(String sourceId, String syntax, String applicationId, String eventType, Map<String, Object> body, int retries) {
        super(sourceId, syntax, applicationId, eventType, body);
        this.retries = retries;
    }

    public void decreaseRetries() {
        if (retries > 0) {
            retries--;
        }
    }

    public int getRetries() {
        return retries;
    }
}