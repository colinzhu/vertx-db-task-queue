package io.github.colinzhu.taskqueue;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.time.Duration;

@RequiredArgsConstructor
@Data
@Accessors(chain = true)
public class TaskPollerConfig<T> {
    private final String queueName;
    private final Class<T> payloadClass;

    private int batchSize = 20;
    private Duration deadline = Duration.ofMinutes(15); // for auto recovery, after the deadline, if the task is still valid (CREATED / PROCESSING), it will be checked out and processed again
    private Duration noTaskPollInterval = Duration.ofSeconds(10);
    private Duration hasTaskPollInterval = Duration.ZERO;
    private Duration errorProcessTasksInterval = Duration.ofSeconds(10);
    private Duration errorCheckOutInterval = Duration.ofSeconds(60);
    private boolean pollNextBatch = true;
}
