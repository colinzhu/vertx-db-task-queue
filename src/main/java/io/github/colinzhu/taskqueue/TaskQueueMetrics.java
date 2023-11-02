package io.github.colinzhu.taskqueue;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.vertx.micrometer.backends.BackendRegistries;

class TaskQueueMetrics {
    private final MeterRegistry registry = BackendRegistries.getDefaultNow();

    Timer pollerTimer(String type, String queueName, String... tags) {
        return Timer.builder("taskqueue.poller." + type).tags("queue",queueName).tags(tags).register(registry);
    }
}
