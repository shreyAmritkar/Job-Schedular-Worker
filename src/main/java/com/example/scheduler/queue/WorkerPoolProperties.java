package com.example.scheduler.queue;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@ConfigurationProperties(prefix = "scheduler.worker-pool")
public class WorkerPoolProperties {

    /** Number of concurrent worker threads. Spec: default 4. */
    private int size = 4;

    /** Approximate wait before retrying when the queue is empty. Spec: ~500ms. */
    private Duration pollBackoff = Duration.ofMillis(500);

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public Duration getPollBackoff() {
        return pollBackoff;
    }

    public void setPollBackoff(Duration pollBackoff) {
        this.pollBackoff = pollBackoff;
    }
}
