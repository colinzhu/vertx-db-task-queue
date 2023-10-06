package io.github.colinzhu.taskqueue.example.release;

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
public class PaymentReleaseVerticle extends AbstractVerticle {
    private TaskPoller<Payment> poller;
    @Override
    public void start() {
        JDBCPool pool = H2Database.getJdbcPool(vertx, false); // TODO check if pool is shared

        // prepare a taskProcessor
        PaymentReleaseTaskProcessor taskProcessor = new PaymentReleaseTaskProcessor(vertx, pool, TaskQueueService.taskQueue(vertx));

        // register taskProcessor to poller
        TaskPollerConfig<Payment> taskPollerConfig = new TaskPollerConfig<>("payment.release", taskProcessor, Payment.class);
        poller = new TaskPoller<>(vertx, pool, taskPollerConfig);
        poller.start();

        log.info("{}[{}] instance started", PaymentReleaseVerticle.class.getSimpleName(), Integer.toHexString(this.hashCode()));
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        poller.stop().onSuccess(v -> stopPromise.complete());
    }
}
