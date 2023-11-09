package io.github.colinzhu.taskqueue;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class TaskProcessorVerticle<T> extends AbstractVerticle {
    private final String queueName;
    private final Supplier<Function<Task<T>, Future<?>>> taskProcessorSupplier;
    private final TaskQueueMetrics metrics = new TaskQueueMetrics();
    private Function<Task<T>, Future<?>> taskProcessor;
    @Override
    public void start() {
        taskProcessor = taskProcessorSupplier.get(); // make sure the processor instance is created by verticle itself, not by another component, so the JDBCPool in the processor will be created by this verticle instance
        vertx.eventBus().consumer(queueName, this::handle);
        log.info("{} created", this);
    }

    private void handle(Message<Task<T>> message) {
        long start = System.currentTimeMillis();
        log.info("{} task received, taskId={}", this, message.body().getId());
        Future.succeededFuture()
                .compose(any -> taskProcessor.apply(message.body()))
                .onSuccess(message1 -> {
                    message.reply(message1);
                    metrics.processorTimer(queueName, "result","success").record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                })
                .onFailure(err -> {
                    log.error("{} error processing task, taskId={}", this, message.body().getId(), err);
                    message.fail(1, "task processor replied err message: " + err.getMessage());
                    metrics.processorTimer(queueName, "result","failure").record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
               });
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        log.info("{} stop triggered", this);
        vertx.setTimer(9000, id -> {
            stopPromise.complete();
            log.info("{} stopped", this);
        });
        vertx.setPeriodic(1000, id -> log.info("{} stopping...", this));
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "-" + queueName + "-" + Integer.toHexString(hashCode());
    }
}
