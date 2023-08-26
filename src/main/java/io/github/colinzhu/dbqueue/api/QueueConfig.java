package io.github.colinzhu.dbqueue.api;

import io.vertx.core.Future;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.util.function.Function;

@RequiredArgsConstructor
@Getter
public class QueueConfig {
    private final String queueName;
    private final int batchSize;
    private final Duration nextProcessDelay;
    private final Function<Task, Future<?>> taskProcessor;
}
