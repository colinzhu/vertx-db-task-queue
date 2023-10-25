package io.github.colinzhu.taskqueue;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.jdbcclient.JDBCPool;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class TaskPollerVerticle<T> extends AbstractVerticle {
    private final Supplier<JDBCPool> poolSupplier;
    private final TaskPollerConfig<T> config;
    private TaskPoller<T> poller;

    @Override
    public void start() {
        // make sure the pool instance is created by verticle itself, not a shared instance created by another component
        poller = new TaskPoller<>(vertx, poolSupplier.get(), config);
        if (config.getToStartPoller().get()) {
            log.info("{} toStartPoller=true, will start the poller", this);
            poller.start();
        } else {
            log.info("{} toStartPoller=false, will NOT start the poller", this);
        }

        vertx.eventBus().consumer("taskqueue.poller.pause." + config.getQueueName(), msg -> poller.stop().onSuccess(v -> log.info("{} paused", this)));
        vertx.eventBus().consumer("taskqueue.poller.start." + config.getQueueName(), msg -> { poller.start(); log.info("{} started", this); });
        log.info("{} created", this);
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        log.info("{} stopping", this);
        poller.stop().onSuccess(v -> stopPromise.complete()).onSuccess(v -> log.info("{} stopped", this));
    }

    @Deprecated
    public Future<Void> stopPoller() {
        return poller.stop();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "-" + config.getQueueName() + "-" + Integer.toHexString(hashCode());
    }
}
