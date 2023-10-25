package io.github.colinzhu.taskqueue;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.time.Duration;
import java.util.function.Supplier;

@RequiredArgsConstructor
@Data
@Accessors(chain = true)
public class TaskPollerConfig<T> {
    private final String queueName;
    private final Class<T> payloadClass;

    private int batchSize = 20;
    private Duration timeout = Duration.ofMinutes(10);
    private Duration nextProcessDelay = Duration.ofMinutes(15); // for auto recovery, start from checkout, after nextProcessDelay, if the task is still valid (CREATED / PROCESSING), it will be checked out and processed again
    private Duration noTaskPollInterval = Duration.ofSeconds(10);
    private Duration hasTaskPollInterval = Duration.ZERO;
    private Duration errorProcessTasksInterval = Duration.ofSeconds(10);
    private Duration errorCheckOutInterval = Duration.ofSeconds(60);
    private boolean pollNextBatch = true;
    private Supplier<Boolean> toStartPoller = () -> Boolean.TRUE;
}
