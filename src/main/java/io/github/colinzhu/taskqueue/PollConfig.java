package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.time.Duration;
import java.util.function.Function;

@RequiredArgsConstructor
@Getter
public class PollConfig {
    private final String queueName;
    private final int batchSize;
    private final Duration nextProcessDelay;
    private final Function<Task, Future<Integer>> taskProcessor;
    @Setter
    private Duration noTaskPollInterval = Duration.ofSeconds(5);
    @Setter
    private Duration hasTaskPollInterval = Duration.ofMillis(1);
    @Setter
    private Duration processErrRetryInterval = Duration.ofSeconds(5);
    @Setter
    private Duration errPollingRetryInterval = Duration.ofSeconds(60);
    @Setter
    private boolean pollNextBatch = true;
    @Override
    public String toString() {
        return "QueueConfig{" +
                "queueName='" + queueName + '\'' +
                '}';
    }
}
