package io.github.colinzhu.taskqueue.example;

import io.github.colinzhu.taskqueue.PollConfig;
import io.github.colinzhu.taskqueue.poller.TaskPoller;
import io.vertx.core.AbstractVerticle;
import io.vertx.jdbcclient.JDBCPool;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Slf4j
@RequiredArgsConstructor
public class PaymentCheckVerticle extends AbstractVerticle {
    private final JDBCPool pool;
    @Override
    public void start() {
        PaymentCheckTaskProcessor taskProcessor = new PaymentCheckTaskProcessor(pool);
        PollConfig pollConfig = new PollConfig("QueuePaymentToBeChecked", 5, Duration.ofMinutes(10), taskProcessor);
        TaskPoller pollerQ1 = new TaskPoller(vertx, pool, pollConfig); // how often to fetch tasks
        pollerQ1.start();
    }
}
