package io.github.colinzhu.taskqueue.example;

import io.github.colinzhu.taskqueue.H2Database;
import io.github.colinzhu.taskqueue.PollConfig;
import io.github.colinzhu.taskqueue.TaskPoller;
import io.github.colinzhu.taskqueue.TaskQueueService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.jdbcclient.JDBCPool;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Slf4j
@RequiredArgsConstructor
public class PaymentReleaseVerticle extends AbstractVerticle {
    private TaskPoller<Payment> poller;
    @Override
    public void start() {
        JDBCPool pool = H2Database.getJdbcPool(vertx, false); // TODO check if pool is shared

        // prepare a taskProcessor
        PaymentReleaseTaskProcessor taskProcessor = new PaymentReleaseTaskProcessor(vertx, pool, TaskQueueService.taskQueue());

        // register taskProcessor to poller
        PollConfig<Payment> pollConfig = PollConfig.<Payment>builder().queueName("payment.release").batchSize(20).nextProcessDelay(Duration.ofMinutes(10)).taskProcessor(taskProcessor).payloadClass(Payment.class).noTaskPollInterval(Duration.ofSeconds(5)).build();
        poller = new TaskPoller<>(vertx, pool, pollConfig);
        poller.start();

        vertx.eventBus().consumer("payment.release", taskProcessor);
        log.info("{}[{}] instance started", PaymentReleaseVerticle.class.getSimpleName(), this.hashCode());
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        poller.stop().onSuccess(v -> stopPromise.complete());
    }
}
