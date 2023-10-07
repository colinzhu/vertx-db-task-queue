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

    @Override
    public void start() {
        String taskPollerVerticleId = "taskPollerVerticle-" + config.getQueueName() + "-" + Integer.toHexString(this.hashCode());
        poller = new TaskPoller<>(vertx, pool, config);
        poller.start();
        log.info("{} created", taskPollerVerticleId);
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        poller.stop().onSuccess(v -> stopPromise.complete());
    }

    public void startPoller() {
        poller.start();
    }

    public Future<Void> stopPoller() {
        return poller.stop();
    }
}
