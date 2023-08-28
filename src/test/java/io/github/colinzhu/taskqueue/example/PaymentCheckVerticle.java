package io.github.colinzhu.taskqueue.example;

import io.github.colinzhu.taskqueue.PollConfig;
import io.github.colinzhu.taskqueue.TaskPoller;
import io.vertx.core.AbstractVerticle;
import io.vertx.jdbcclient.JDBCPool;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Slf4j
@RequiredArgsConstructor
public class PaymentCheckVerticle extends AbstractVerticle {
    private JDBCPool pool;
    private TaskPoller<Payment> poller;
    @Override
    public void start() {
        pool = H2Database.getJdbcPool(vertx); // TODO check if pool is shared
        log.info("{}-{} {} started", PaymentCheckVerticle.class.getName(), this.hashCode(), pool.hashCode());
        PaymentCheckTaskProcessor taskProcessor = new PaymentCheckTaskProcessor(vertx, pool);
        PollConfig pollConfig = new PollConfig("QueuePaymentToBeChecked", 5, Duration.ofMinutes(10), taskProcessor);
        vertx.eventBus().consumer(pollConfig.getQueueName(), taskProcessor);

        poller = new TaskPoller<>(vertx, pool, pollConfig); // how often to fetch tasks
        poller.start();
    }
}
