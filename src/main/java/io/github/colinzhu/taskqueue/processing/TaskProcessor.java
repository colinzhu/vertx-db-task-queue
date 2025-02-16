package io.github.colinzhu.taskqueue.processing;

import io.vertx.core.Future;

public interface TaskProcessor<T> {
    Future<?> process(Task<T> task);
}
