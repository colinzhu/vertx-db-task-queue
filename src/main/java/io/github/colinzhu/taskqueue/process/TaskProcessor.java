package io.github.colinzhu.taskqueue.process;

import io.vertx.core.Future;

public interface TaskProcessor<T> {
    Future<Void> process(Task<T> task);
}
