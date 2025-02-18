package io.github.colinzhu.taskqueue;

import ch.qos.logback.classic.Level;
import io.github.colinzhu.taskqueue.dispatch.TaskDispatchConfig;
import io.github.colinzhu.taskqueue.enqueue.TaskEnqueueService;
import io.github.colinzhu.taskqueue.example.Payment;
import io.github.colinzhu.taskqueue.internal.TaskStatus;
import io.github.colinzhu.taskqueue.process.TaskProcessService;
import io.github.colinzhu.taskqueue.process.TaskProcessor;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.sqlclient.SqlConnection;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.colinzhu.taskqueue.TaskQueueTestUtils.*;
import static io.github.colinzhu.taskqueue.internal.TaskStatus.COMPLETED;
import static org.slf4j.Logger.ROOT_LOGGER_NAME;

@ExtendWith(VertxExtension.class)
@Slf4j
class TaskQueueByOtherDispatcherTest {
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

        taskEnqueueService = TaskEnqueueService.getInstance(vertx);
        taskProcessService = TaskProcessService.getInstance();
        Database db = Database.get(Database.H2_MEM);
        pool = db.getJdbcPool(vertx);
        db.createTables(pool).onComplete(ar -> testContext.completeNow());
        poolSupplier = () -> db.getJdbcPool(vertx);

    }

    @ParameterizedTest
    @DisplayName("Task already completed / deleted by another poller")
    @CsvSource({"COMPLETE", "COMPLETE_DELETE"})
    void testTaskAlreadyCompletedDeletedByAnotherPoller(String action, Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        Payment payment = new Payment("CREATED", OffsetDateTime.now());

        TaskProcessor<Payment> taskProcessor = createTaskProcessorForAnotherPoller(action);
        String queueName = getQueueName(action);
        TaskDispatchConfig<Payment> config = createTaskDispatchConfig(queueName);

        String taskRef = "ref-already-" + System.currentTimeMillis() + action;
        deployDispatchAndProcessVerticles(vertx, config, taskProcessor, poolSupplier)
                .compose(any -> savePaymentAndEnqueue(queueName, payment, taskRef))
                .onComplete(testContext.succeeding(any -> verifyTaskCompletionForAnotherPoller(vertx, testContext, checkpoint, payment, action, taskRef)));
    }

    private TaskProcessor<Payment> createTaskProcessorForAnotherPoller(String action) {
        return task -> {
            Future<Void> futureOfAnother;
            if ("COMPLETE_DELETE".equals(action)) {
                futureOfAnother = pool.withTransaction(conn ->
                        updatePaymentTo(conn, task.getPayload(), "STATUS_ANOTHER_POLLER")
                                .compose(p -> taskProcessService.completeDelete(conn, task)));
            } else {
                futureOfAnother = pool.withTransaction(conn ->
                        updatePaymentTo(conn, task.getPayload(), "STATUS_ANOTHER_POLLER")
                                .compose(p -> taskProcessService.complete(conn, task)));
            }

            Function<SqlConnection, Future<Void>> function = conn -> updatePaymentTo(conn, task.getPayload(), "STATUS_CURRENT_POLLER")
                    .compose(p -> taskProcessService.reenqueue(conn, task, Duration.ofSeconds(5)));

            return futureOfAnother.compose(any -> pool.withTransaction(function));
        };
    }


    private Future<Void> savePaymentAndEnqueue(String queueName, Payment payment, String taskRef) {
        return pool.withTransaction(conn -> savePayment(conn, payment).compose(pay -> taskEnqueueService.enqueue(conn, queueName, taskRef, pay)));
    }


    private void verifyTaskCompletionForAnotherPoller(Vertx vertx, VertxTestContext testContext, Checkpoint checkpoint, Payment payment, String action, String taskRef) {
        vertx.setTimer(1000, id -> {
            retrievePayment(payment.getId(), pool).onComplete(testContext.succeeding(res -> {
                Assertions.assertEquals("STATUS_ANOTHER_POLLER", res.getStatus(), "PAYMENT status should still be STATUS_ANOTHER_POLLER instead of STATUS_CURRENT_POLLER");
                checkpoint.flag();
            }));

            retrieveTask(taskRef, pool).onComplete(testContext.succeeding(res -> {
                if ("COMPLETE_DELETE".equals(action)) {
                    Assertions.assertNull(res, "task should not be available, already deleted by another poller");
                } else {
                    Assertions.assertEquals(COMPLETED, TaskStatus.valueOf(res.getString("STATUS")));
                }
                checkpoint.flag();
            }));
        });
    }

}