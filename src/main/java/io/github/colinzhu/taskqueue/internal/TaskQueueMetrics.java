package io.github.colinzhu.taskqueue.internal;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.vertx.micrometer.backends.BackendRegistries;
import lombok.NoArgsConstructor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@NoArgsConstructor
public class TaskQueueMetrics {
    private final MeterRegistry registry = BackendRegistries.getDefaultNow();

    public void recordTime(String name, String queueName, long startTime, String... tags) {
        if (registry == null) {
            return;
        }
        Timer.builder(name).tags("queue",queueName).tags(tags).register(registry)
                .record(System.currentTimeMillis() - startTime, MILLISECONDS);
    }
}
