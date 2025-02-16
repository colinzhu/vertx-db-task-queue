package io.github.colinzhu.taskqueue.process;

import io.vertx.core.Future;

public interface TaskProcessor<T> {
    Future<?> process(Task<T> task);
}
