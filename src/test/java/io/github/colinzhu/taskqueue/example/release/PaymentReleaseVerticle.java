package io.github.colinzhu.taskqueue.example.release;

import io.github.colinzhu.taskqueue.H2Database;
import io.github.colinzhu.taskqueue.PollConfig;
import io.github.colinzhu.taskqueue.TaskPoller;
import io.github.colinzhu.taskqueue.TaskQueueService;
import io.github.colinzhu.taskqueue.example.Payment;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.jdbcclient.JDBCPool;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Slf4j
public class PaymentReleaseVerticle extends AbstractVerticle {
    private TaskPoller<Payment> poller;
    @Override
    public void start() {
        JDBCPool pool = H2Database.getJdbcPool(vertx, false); // TODO check if pool is shared

        // prepare a taskProcessor
        PaymentReleaseTaskProcessor taskProcessor = new PaymentReleaseTaskProcessor(vertx, pool, TaskQueueService.taskQueue(vertx));

        // register taskProcessor to poller
        PollConfig<Payment> pollConfig = PollConfig.<Payment>builder()
                .queueName("payment.release")
                .batchSize(20)
                .nextProcessDelay(Duration.ofMinutes(10))
                .taskProcessor(taskProcessor)
                .payloadClass(Payment.class)
                .noTaskPollInterval(Duration.ofSeconds(10))
                .build();
        poller = new TaskPoller<>(vertx, pool, pollConfig);
        poller.start();

        log.info("{}[{}] instance started", PaymentReleaseVerticle.class.getSimpleName(), Integer.toHexString(this.hashCode()));
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        poller.stop().onSuccess(v -> stopPromise.complete());
    }
}
