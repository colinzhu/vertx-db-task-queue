package io.github.colinzhu.taskqueue.example.check;

import io.github.colinzhu.taskqueue.H2Database;
import io.github.colinzhu.taskqueue.TaskPoller;
import io.github.colinzhu.taskqueue.TaskPollerConfig;
import io.github.colinzhu.taskqueue.TaskQueueService;
import io.github.colinzhu.taskqueue.example.Payment;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.jdbcclient.JDBCPool;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PaymentCheckVerticle extends AbstractVerticle {
    private TaskPoller<Payment> poller;
    @Override
    public void start() {
        JDBCPool pool = H2Database.getJdbcPool(vertx, false); // TODO check if pool is shared

        // prepare a taskProcessor
        PaymentCheckTaskProcessor taskProcessor = new PaymentCheckTaskProcessor(vertx, pool,  TaskQueueService.taskQueue(vertx));

        // register taskProcessor to poller
        TaskPollerConfig<Payment> taskPollerConfig = new TaskPollerConfig<>("payment.check", taskProcessor, Payment.class);
        poller = new TaskPoller<>(vertx, pool, taskPollerConfig);
        poller.start();

        log.info("{}[{}] instance started", PaymentCheckVerticle.class.getSimpleName(), Integer.toHexString(this.hashCode()));
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        poller.stop().onSuccess(v -> stopPromise.complete());
    }
}
