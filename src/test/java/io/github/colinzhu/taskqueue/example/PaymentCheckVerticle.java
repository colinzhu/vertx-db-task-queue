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

        // prepare a taskProcessor
        PaymentCheckTaskProcessor taskProcessor = new PaymentCheckTaskProcessor(vertx, pool);

        // register taskProcessor to poller
        PollConfig<Payment> pollConfig = new PollConfig<>("payment.check", 5, Duration.ofMinutes(10), taskProcessor, Payment.class);
        poller = new TaskPoller<>(vertx, pool, pollConfig);
        poller.start();

        // register taskProcessor to event bus
        vertx.eventBus().consumer("payment.check", taskProcessor);
        log.info("{}[{}] instance started", PaymentCheckVerticle.class.getSimpleName(), this.hashCode());
    }
}
