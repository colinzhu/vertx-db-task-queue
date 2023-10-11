package io.github.colinzhu.taskqueue;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.jdbcclient.JDBCPool;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class TaskPollerVerticle<T> extends AbstractVerticle {
    private final JDBCPool pool;
    private final TaskPollerConfig<T> config;
    private TaskPoller<T> poller;
    private String logId;

    @Override
    public void start() {
        logId = "taskPollerVerticle-" + config.getQueueName() + "-" + Integer.toHexString(this.hashCode());
        poller = new TaskPoller<>(vertx, pool, config);
        poller.start();
        log.info("{} created", logId);
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        log.info("{} stopping", logId);
        poller.stop().onSuccess(v -> stopPromise.complete()).onSuccess(v -> log.info("{} stopped", logId));
    }

    public void startPoller() {
        poller.start();
    }

    public Future<Void> stopPoller() {
        return poller.stop();
    }
}
