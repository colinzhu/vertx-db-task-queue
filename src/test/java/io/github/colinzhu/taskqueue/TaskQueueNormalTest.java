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
import static io.github.colinzhu.taskqueue.internal.TaskStatus.CREATED;
import static org.slf4j.Logger.ROOT_LOGGER_NAME;

@ExtendWith(VertxExtension.class)
@Slf4j
class TaskQueueNormalTest {
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


    @ParameterizedTest
    @DisplayName("Normal - within one transaction, business entity object updated, tasks finished (deleted)")
    @CsvSource({"COMPLETE", "REENQUEUE", "COMPLETE_DELETE"})
    void testNormal(String action, Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);

        Payment payment = new Payment("CREATED", OffsetDateTime.now());
        TaskProcessor<Payment> taskProcessor = createTaskProcessor(action, payment);
        String queueName = getQueueName(action);
        TaskDispatchConfig<Payment> config = createTaskDispatchConfig(queueName);

        String taskRef = "ref" + System.currentTimeMillis() + action;
        deployDispatchAndProcessVerticles(vertx, config, taskProcessor, poolSupplier)
                .compose(any -> savePaymentAndEnqueue(queueName, payment, taskRef))
                .onComplete(testContext.succeeding(any -> verifyTaskCompletion(vertx, testContext, checkpoint, payment, action, taskRef)));
    }


    private TaskProcessor<Payment> createTaskProcessor(String action, Payment payment) {
        return task -> {
            Assertions.assertEquals(payment.getCreateTime().toInstant(), task.getPayload().getCreateTime().toInstant(),
                    "the task payload object should be same as the original object");
            log.info("Processing {}", task.getPayload());

            return pool.withConnection(conn -> {
                Function<Integer, Future<Void>> func = switch (action) {
                    case "REENQUEUE" -> p -> taskProcessService.reenqueue(conn, task, Duration.ofSeconds(5));
                    case "COMPLETE" -> p -> taskProcessService.complete(conn, task);
                    case "COMPLETE_DELETE" -> p -> taskProcessService.completeDelete(conn, task);
                    default -> null;
                };
                return TaskQueueTestUtils.updatePayment(conn, task.getPayload()).compose(func);
            });
        };
    }

    private TaskDispatchConfig<Payment> createTaskDispatchConfig(String queueName) {
        return new TaskDispatchConfig<>(queueName, Payment.class)
                .setNoTaskPollInterval(Duration.ofMillis(500));
    }

    private Future<Void> savePaymentAndEnqueue(String queueName, Payment payment, String taskRef) {
        return pool.withTransaction(conn -> savePayment(conn, payment).compose(pay -> taskEnqueueService.enqueue(conn, queueName, taskRef, pay)));
    }

    private void verifyTaskCompletion(Vertx vertx, VertxTestContext testContext, Checkpoint checkpoint, Payment payment, String action, String taskRef) {
        vertx.setTimer(1000, id -> {
            TaskQueueTestUtils.retrievePayment(payment.getId(), pool).onComplete(testContext.succeeding(res -> {
                Assertions.assertEquals("PROCESSED", res.getStatus(), "PAYMENT status should be changed");
                checkpoint.flag();
            }));

            TaskQueueTestUtils.retrieveTask(taskRef, pool).onComplete(testContext.succeeding(res -> {
                if ("REENQUEUE".equals(action)) {
                    Assertions.assertSame(CREATED, TaskStatus.valueOf(res.getString("STATUS")), "task should still be available for next process");
                } else if ("COMPLETE".equals(action)) {
                    Assertions.assertSame(COMPLETED, TaskStatus.valueOf(res.getString("STATUS")), "task should still be in COMPLETED status");
                } else {
                    Assertions.assertNull(res, "task should be deleted");
                }
                checkpoint.flag();
            }));
        });
    }

}