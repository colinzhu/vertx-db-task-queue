package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import lombok.*;

import java.time.Duration;
import java.util.function.Function;

//@Data
@Value
@Builder
@RequiredArgsConstructor
public class PollConfig<T> {
    String queueName;
    int batchSize;
    Duration nextProcessDelay;
    Function<Task<T>, Future<Integer>> taskProcessor;
    Class<T> payloadClass;

    @Builder.Default Duration noTaskPollInterval = Duration.ofSeconds(5);
    @Builder.Default Duration hasTaskPollInterval = Duration.ofMillis(1);
    @Builder.Default Duration errorProcessTasksInterval = Duration.ofSeconds(5);
    @Builder.Default Duration errorCheckOutInterval = Duration.ofSeconds(60);
    @Builder.Default boolean pollNextBatch = true;
}
