package io.github.colinzhu.taskqueue;

import ch.qos.logback.classic.Level;
import io.github.colinzhu.taskqueue.dispatch.TaskDispatchConfig;
import io.github.colinzhu.taskqueue.enqueue.TaskEnqueueService;
import io.github.colinzhu.taskqueue.example.Payment;
import io.github.colinzhu.taskqueue.internal.EventAddress;
import io.github.colinzhu.taskqueue.process.TaskProcessService;
import io.github.colinzhu.taskqueue.process.TaskProcessor;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.function.Supplier;

import static io.github.colinzhu.taskqueue.TaskQueueTestUtils.*;
import static org.slf4j.Logger.ROOT_LOGGER_NAME;

@ExtendWith(VertxExtension.class)
@Slf4j
class TaskQueueDispatcherStoppedTest {
    private static JDBCPool pool;
    private static Supplier<JDBCPool> poolSupplier;
    private static TaskEnqueueService taskEnqueueService;
    private static TaskProcessService taskProcessService;

    @BeforeAll
    static void init(Vertx vertx, VertxTestContext testContext) {
        setLogLevel(ROOT_LOGGER_NAME, Level.INFO);
        setLogLevel("com.mchange.v2.resourcepool.BasicResourcePool", Level.INFO);
        setLogLevel("io.github.colinzhu.taskqueue", Level.DEBUG);
        setLogLevel("io.github.colinzhu.taskqueue.internal.TaskRepo", Level.DEBUG);
        setLogLevel("io.github.colinzhu.taskqueue.dispatch.TaskDispatcher", Level.DEBUG);

        taskEnqueueService = TaskEnqueueService.taskQueue(vertx);
        taskProcessService = TaskProcessService.getInstance();
        Database db = Database.get(Database.H2_MEM);
        pool = db.getJdbcPool(vertx);
        db.createTables(pool).onComplete(ar -> testContext.completeNow());
        poolSupplier = () -> db.getJdbcPool(vertx);

    }

    @Test
    @DisplayName("Poller is stopped - no task will be picked up")
    void testDispatcherStopped(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);

        String queueName = "Q3-poller-stopped";
        TaskProcessor<Payment> taskProcessor = createTaskProcessorForPollerStopped();
        TaskDispatchConfig<Payment> config = createTaskDispatchConfig(queueName);

        String taskRef = queueName + System.currentTimeMillis();
        deployDispatchAndProcessVerticles(vertx, config, taskProcessor, poolSupplier)
                .map(any -> stopPoller(vertx, config.getQueueName()))
                .onSuccess(any -> enqueueTaskAfterPollerStopped(vertx, testContext, checkpoint, queueName, taskRef));
    }


    private TaskProcessor<Payment> createTaskProcessorForPollerStopped() {
        return task -> pool.withTransaction(conn -> updatePayment(conn, task.getPayload())
                .compose(p -> taskProcessService.reenqueue(conn, task, Duration.ofSeconds(5))));
    }

    private Object stopPoller(Vertx vertx, String queueName) {
        return vertx.eventBus().publish(EventAddress.POLLER_PAUSE_PREFIX + queueName, null);
    }

    private void enqueueTaskAfterPollerStopped(Vertx vertx, VertxTestContext testContext, Checkpoint checkpoint, String queueName, String taskRef) {
        Payment payment = new Payment("CREATED", OffsetDateTime.now());
        Future<Void> enqueueTask = pool.withTransaction(conn -> savePayment(conn, payment).compose(p -> taskEnqueueService.enqueue(conn, queueName, taskRef, p)));

        enqueueTask.onComplete(testContext.succeeding(taskId -> {
            vertx.setTimer(1000, id -> {
                retrievePayment(payment.getId(), pool).onComplete(testContext.succeeding(res -> {
                    Assertions.assertEquals("CREATED", res.getStatus(), "PAYMENT should not be changed");
                    checkpoint.flag();
                }));

                retrieveTask(taskRef, pool).onComplete(testContext.succeeding(res -> {
                    Assertions.assertEquals("CREATED", res.getString("STATUS"), "task should not be checked-out");
                    checkpoint.flag();
                }));
            });
        }));
    }

}