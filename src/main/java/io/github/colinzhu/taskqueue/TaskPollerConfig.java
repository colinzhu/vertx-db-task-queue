package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import lombok.*;
import lombok.experimental.Accessors;

import java.time.Duration;
import java.util.function.Function;

@RequiredArgsConstructor
@Data
@Accessors(chain = true)
public class TaskPollerConfig<T> {
    private final String queueName;
    private final Function<Task<T>, Future<?>> taskProcessor;
    private final Class<T> payloadClass;

    private int batchSize = 20;
    private Duration deadline = Duration.ofMinutes(15); // for auto recovery, after the deadline, if the task is still valid (CREATED / PROCESSING), it will be checked out and processed again
    private Duration noTaskPollInterval = Duration.ofSeconds(5);
    private Duration hasTaskPollInterval = Duration.ZERO;
    private Duration errorProcessTasksInterval = Duration.ofSeconds(5);
    private Duration errorCheckOutInterval = Duration.ofSeconds(60);
    private boolean pollNextBatch = true;
}
