package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import lombok.*;

import java.time.Duration;
import java.util.function.Function;

@Data
public class PollConfig<T> {
    private final String queueName;
    private final int batchSize;
    private final Duration nextProcessDelay;
    private final Function<Task<T>, Future<Integer>> taskProcessor;
    private final Class<T> payloadClass;

    private Duration noTaskPollInterval = Duration.ofSeconds(5);
    private Duration hasTaskPollInterval = Duration.ofMillis(1);
    private Duration errorProcessTasksInterval = Duration.ofSeconds(5);
    private Duration errorCheckOutInterval = Duration.ofSeconds(60);
    private boolean pollNextBatch = true;
}
