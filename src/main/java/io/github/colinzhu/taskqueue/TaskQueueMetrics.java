package io.github.colinzhu.taskqueue;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.vertx.micrometer.backends.BackendRegistries;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class TaskQueueMetrics {
    private final MeterRegistry registry = BackendRegistries.getDefaultNow();

    void recordTime(String name, String queueName, long startTime, String... tags) {
        if (registry == null) {
            return;
        }
        Timer.builder(name).tags("queue",queueName).tags(tags).register(registry)
                .record(System.currentTimeMillis() - startTime, MILLISECONDS);
    }
}
