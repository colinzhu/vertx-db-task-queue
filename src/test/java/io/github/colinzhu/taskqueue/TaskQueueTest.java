package io.github.colinzhu.taskqueue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.github.colinzhu.taskqueue.enqueue.TaskEnqueueService;
import io.github.colinzhu.taskqueue.example.Payment;
import io.github.colinzhu.taskqueue.internal.TaskStatus;
import io.github.colinzhu.taskqueue.polling.TaskPollerConfig;
import io.github.colinzhu.taskqueue.polling.TaskPollerVerticle;
import io.github.colinzhu.taskqueue.processing.TaskProcessService;
import io.github.colinzhu.taskqueue.processing.TaskProcessor;
import io.github.colinzhu.taskqueue.processing.TaskProcessorVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.sqlclient.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.colinzhu.taskqueue.internal.TaskStatus.COMPLETED;
import static io.github.colinzhu.taskqueue.internal.TaskStatus.CREATED;
import static org.slf4j.Logger.ROOT_LOGGER_NAME;

@ExtendWith(VertxExtension.class)
@Slf4j
class TaskQueueTest {
    private static JDBCPool pool;
    private static Supplier<JDBCPool> poolSupplier;
    private static TaskEnqueueService taskEnqueueService;
    private static TaskProcessService taskProcessService;
    private static final String POLLER_PAUSE_PREFIX = "taskqueue.poller.pause.";
    @BeforeAll
    static void init(Vertx vertx, VertxTestContext testContext) {
        setLogLevel(ROOT_LOGGER_NAME, Level.INFO);
        setLogLevel("com.mchange.v2.resourcepool.BasicResourcePool", Level.INFO);
        setLogLevel("io.github.colinzhu.taskqueue", Level.DEBUG);
        setLogLevel("io.github.colinzhu.taskqueue.internal.TaskRepo", Level.DEBUG);
        setLogLevel("io.github.colinzhu.taskqueue.polling.TaskPoller", Level.DEBUG);

        taskEnqueueService = TaskEnqueueService.taskQueue(vertx);
        taskProcessService = TaskProcessService.getInstance();
        Database db = Database.get(Database.H2_MEM);
        pool = db.getJdbcPool(vertx);
        db.createTables(pool).onComplete(ar -> testContext.completeNow());
        poolSupplier = () -> db.getJdbcPool(vertx);

    }

    private static void setLogLevel(String logger, Level level) {
        Logger taskQueueLogger = (Logger) LoggerFactory.getLogger(logger);
        taskQueueLogger.setLevel(level);
    }

    @AfterEach
    void tearDown() {
    }

    @ParameterizedTest
    @DisplayName("Normal - within one transaction, business entity object updated, tasks finished (deleted)")
    @CsvSource({"COMPLETE", "REENQUEUE", "COMPLETE_DELETE"})
    void testNormal(String afterProcessAction, Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);

        // prepare one object to be put into the queue
        Payment payment = new Payment("CREATED", OffsetDateTime.now());

        // prepare a task processor
        TaskProcessor<Payment> taskProcessor = task -> {
            Assertions.assertEquals(payment.getCreateTime().toInstant(), task.getPayload().getCreateTime().toInstant(), "the task payload object should be same as the original object");
            log.info("Processing {}", task.getPayload());
            Function<SqlConnection, Future<Integer>> function;
            if ("REENQUEUE".equals(afterProcessAction)) {
                function = conn -> updatePayment(conn, task.getPayload()).compose(p -> taskProcessService.reenqueue(conn, task, Duration.ofSeconds(5))); // when verify in 1 sec, not yet in processing status
            } else if ("COMPLETE".equals(afterProcessAction)) {
                function = conn -> updatePayment(conn, task.getPayload()).compose(p -> taskProcessService.complete(conn, task));
            } else {
                function = conn -> updatePayment(conn, task.getPayload()).compose(p -> taskProcessService.completeDelete(conn, task));
            }
            return pool.withTransaction(function);
        };

        // start a poller
        String queueName = switch (afterProcessAction) {
            case "COMPLETE" -> "Q1-need-complete";
            case "REENQUEUE" -> "Q2-need-reenquueue";
            case "COMPLETE_DELETE" -> "Q3-need-completeDelete";
            default -> null;
        };
        TaskPollerConfig<Payment> taskPollerConfig = new TaskPollerConfig<>(queueName, Payment.class)
                .setNoTaskPollInterval(Duration.ofMillis(500)); // make sure it's smaller then the waiting time in verification

        // deploy TaskPollerVerticle and TaskProcessorVerticle
        Future<String> deployVerticles = vertx.deployVerticle(() -> new TaskProcessorVerticle<>(taskPollerConfig.getQueueName(), () -> taskProcessor), new DeploymentOptions().setInstances(1))
                .compose(any -> vertx.deployVerticle(() -> new TaskPollerVerticle<>(poolSupplier, taskPollerConfig), new DeploymentOptions().setInstances(1)));

        // enqueue a task
        log.info("Payment created: {}", payment);
        Future<Long> enqueueTask = deployVerticles.compose(any -> pool.withTransaction(conn -> savePayment(conn, payment).compose(p -> taskEnqueueService.enqueue(conn, queueName, "ref1", p))));

        // verify after the poller processing the task
        enqueueTask.onComplete(testContext.succeeding(taskId -> {
            vertx.setTimer(1000, id -> { // make sure the poller has already processed the task
                // verify payment status
                retrievePayment(payment.getId()).onComplete(testContext.succeeding(res -> {
                    Assertions.assertEquals("PROCESSED", res.getStatus(), "PAYMENT status should be changed");
                    checkpoint.flag();
                }));

                // verify task
                retrieveTask(taskId).onComplete(testContext.succeeding(res -> {
                    if ("REENQUEUE".equals(afterProcessAction)) {
                        Assertions.assertSame(CREATED, TaskStatus.valueOf(res.getString("STATUS")), "task should still be available for next processing");
                    } else if ("COMPLETE".equals(afterProcessAction)) {
                        Assertions.assertSame(COMPLETED, TaskStatus.valueOf(res.getString("STATUS")), "task should still be in COMPLETED status");
                    } else {
                        Assertions.assertNull(res, "task should be deleted");
                    }
                    checkpoint.flag();
                }));
            });
        }));
    }

    @ParameterizedTest
    @DisplayName("Task already completed / deleted by another poller")
    @CsvSource({"COMPLETE", "COMPLETE_DELETE"})
    void testTaskAlreadyCompletedDeletedByAnotherPoller(String afterProcessAction, Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        Payment payment = new Payment("CREATED", OffsetDateTime.now());

        // prepare a task processor
        TaskProcessor<Payment> taskProcessor = task -> {
            // simulate task already finished by another poller (payment status updated to "ABC", task deleted
            Future<Integer> futureOfAnother;
            if ("COMPLETE_DELETE".equals(afterProcessAction)) {
                futureOfAnother = pool.withTransaction(conn -> updatePaymentTo(conn, task.getPayload(), "STATUS_ANOTHER_POLLER").compose(p -> taskProcessService.completeDelete(conn, task)));
            } else {
                futureOfAnother = pool.withTransaction(conn -> updatePaymentTo(conn, task.getPayload(), "STATUS_ANOTHER_POLLER").compose(p -> taskProcessService.complete(conn, task)));
            }

            Function<SqlConnection, Future<Integer>> function = conn -> updatePaymentTo(conn, task.getPayload(), "STATUS_CURRENT_POLLER")
                    .compose(p -> taskProcessService.reenqueue(conn, task, Duration.ofSeconds(5)));

            return futureOfAnother.compose(any -> pool.withTransaction(function));
        };

        // prepare a poller
        String queueName = switch (afterProcessAction) {
            case "COMPLETE" -> "Q-already-completed-by-another-poller";
            case "COMPLETE_DELETE" -> "Q-already-completed-deleted-by-another-poller";
            default -> null;
        };
        TaskPollerConfig<Payment> taskPollerConfig = new TaskPollerConfig<>(queueName, Payment.class)
                .setNoTaskPollInterval(Duration.ofMillis(500)); // make sure it's smaller then the waiting time in verification

        // deploy TaskPollerVerticle and TaskProcessorVerticle
        Future<String> deployVerticles = vertx.deployVerticle(() -> new TaskProcessorVerticle<>(taskPollerConfig.getQueueName(), () -> taskProcessor), new DeploymentOptions().setInstances(1))
                .compose(any -> vertx.deployVerticle(() -> new TaskPollerVerticle<>(poolSupplier, taskPollerConfig), new DeploymentOptions().setInstances(1)));

        // enqueue a task
        Future<Long> enqueueTask = deployVerticles.compose(any -> pool.withTransaction(conn -> savePayment(conn, payment).compose(p -> taskEnqueueService.enqueue(conn, queueName, "ref1", p))));

        // verify after the poller processing the task
        enqueueTask.onComplete(testContext.succeeding(taskId -> {
            vertx.setTimer(1000, id -> { // make sure the poller has already processed the task
                // verify payment status
                retrievePayment(payment.getId()).onComplete(testContext.succeeding(res -> {
                    Assertions.assertEquals("STATUS_ANOTHER_POLLER", res.getStatus(), "PAYMENT status should still be STATUS_ANOTHER_POLLER instead of STATUS_CURRENT_POLLER");
                    checkpoint.flag();
                }));

                // verify task
                retrieveTask(taskId).onComplete(testContext.succeeding(res -> {
                    if ("COMPLETE_DELETE".equals(afterProcessAction)) {
                        Assertions.assertNull(res, "task should not be available, already deleted by another poller");
                    } else {
                        Assertions.assertEquals(COMPLETED, TaskStatus.valueOf(res.getString("STATUS")));
                    }
                    checkpoint.flag();
                }));
            });
        }));
    }

    @Test
    @DisplayName("Poller is stopped - no task will be picked up")
    void testPollerStopped(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);

        // prepare a task processor
        TaskProcessor<Payment> taskProcessor = task -> {
            log.info("Processing {}", task.getPayload());
            return pool.withTransaction(conn -> updatePayment(conn, task.getPayload())
                    .compose(p -> taskProcessService.reenqueue(conn, task, Duration.ofSeconds(5))));
        };

        // prepare a poller
        TaskPollerConfig<Payment> taskPollerConfig = new TaskPollerConfig<>("Q3-poller-stopped", Payment.class)
                .setNoTaskPollInterval(Duration.ofMillis(500));  // make sure it's smaller then the waiting time in verification

        // deploy TaskPollerVerticle and TaskProcessorVerticle
        TaskPollerVerticle<Payment> pollerVerticle = new TaskPollerVerticle<>(poolSupplier, taskPollerConfig);
        Future<String> deployVerticles = vertx.deployVerticle(() -> new TaskProcessorVerticle<>(taskPollerConfig.getQueueName(), () -> taskProcessor), new DeploymentOptions().setInstances(1))
                .compose(any -> vertx.deployVerticle(pollerVerticle));

        deployVerticles.onSuccess(any -> {
            vertx.eventBus().publish(POLLER_PAUSE_PREFIX + taskPollerConfig.getQueueName(), null);

            // stop the poller
//            vertx.setTimer(1000, id -> pollerVerticle.stopPoller()
//                    .onSuccess(v -> log.info("poller stopped."))
//                    .onSuccess(v -> checkpoint.flag())
                    vertx.setTimer(1000, id -> {// make sure the poller has already processed the task
                        // after the poller is stopped, enqueue a task
                        Payment payment = new Payment("CREATED", OffsetDateTime.now());
                        Future<Long> enqueueTask = pool.withTransaction(conn -> savePayment(conn, payment).compose(p -> taskEnqueueService.enqueue(conn, "Q3-poller-stopped", "ref1", p)));

                        // expect the task should not be processed
                        enqueueTask.onComplete(testContext.succeeding(taskId -> {
                            // verify payment status
                            retrievePayment(payment.getId()).onComplete(testContext.succeeding(res -> {
                                Assertions.assertEquals("CREATED", res.getStatus(), "PAYMENT should not be changed");
                                checkpoint.flag();
                            }));

                            // verify task
                            retrieveTask(taskId).onComplete(testContext.succeeding(res -> {
                                Assertions.assertEquals("CREATED", res.getString("STATUS"), "task should not be checked-out");
                                checkpoint.flag();
                            }));
                        }));
                    });
//            );
        });
    }

    @ParameterizedTest
    @DisplayName("Task processing error - changes should be rolled back, tasks should be updated to ERROR")
    @CsvSource({"ERR_IN_TXN", "ERR_BEFORE_TXN"})
    void testTaskProcessingError(String errLocation, Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        Payment payment = new Payment("CREATED", OffsetDateTime.now());

        // prepare a task processor
        TaskProcessor<Payment> taskProcessor = task -> {
            log.info("Processing {}", task.getPayload());
            if ("ERR_BEFORE_TXN".equals(errLocation)) {
                throw new RuntimeException("simulate exception before transaction");
            }
            Function<SqlConnection, Future<Integer>> updatePaymentFunc = conn -> updatePayment(conn, payment)
                    .map(updateCount -> {
                        if ("ERR_IN_TXN".equals(errLocation)) {
                            throw new RuntimeException("simulate exception within transaction");
                        }
                        return updateCount;
                    }).compose(count -> taskProcessService.complete(conn, task));
            return pool.withTransaction(updatePaymentFunc);
        };

        // prepare a poller
        String queueName = switch (errLocation) {
            case "ERR_IN_TXN" -> "Q-ERR_IN_TXN";
            case "ERR_BEFORE_TXN" -> "Q-ERR_BEFORE_TXN";
            default -> null;
        };
        TaskPollerConfig<Payment> taskPollerConfig = new TaskPollerConfig<>(queueName, Payment.class)
                .setNoTaskPollInterval(Duration.ofMillis(500));  // make sure it's smaller then the waiting time in verification

        // deploy TaskPollerVerticle and TaskProcessorVerticle
        Future<String> deployVerticles = vertx.deployVerticle(() -> new TaskProcessorVerticle<>(taskPollerConfig.getQueueName(), () -> taskProcessor), new DeploymentOptions().setInstances(1))
                .compose(any -> vertx.deployVerticle(() -> new TaskPollerVerticle<>(poolSupplier, taskPollerConfig), new DeploymentOptions().setInstances(1)));

        // enqueue a task
        Future<Long> enqueueTask = deployVerticles.compose(any -> pool.withTransaction(conn -> savePayment(conn, payment).compose(p -> taskEnqueueService.enqueue(conn, queueName, "ref1", p))));

        // verify after the poller processing the task
        enqueueTask.onComplete(testContext.succeeding(taskId -> {
            vertx.setTimer(1000, id -> { // make sure the poller has already processed the task
                // verify payment status
                retrievePayment(payment.getId()).onComplete(testContext.succeeding(p -> {
                    Assertions.assertEquals("CREATED", p.getStatus(), "PAYMENT status should be rolled back / no change");
                    checkpoint.flag();
                }));

                // verify task
                retrieveTask(taskId).onComplete(testContext.succeeding(row -> {
                    Assertions.assertEquals("ERROR", row.getString("STATUS"), "task should be updated to ERROR");
                    checkpoint.flag();
                }));
            });
        }));
    }

    private Future<Payment> savePayment(SqlConnection sqlConnection, Payment payment) {
        return sqlConnection.preparedQuery("insert into PAYMENT (STATUS, CREATE_TIME) values (?, ?)")
                .execute(Tuple.of(payment.getStatus(), payment.getCreateTime()))
                .map(rows -> {
                    Long id = rows.property(JDBCPool.GENERATED_KEYS).getLong(0);
                    payment.setId(id);
                    return payment;
                });
    }

    private Future<Integer> updatePayment(SqlConnection sqlConnection, Payment payment) {
        return sqlConnection.query("UPDATE PAYMENT SET STATUS = 'PROCESSED' WHERE ID = " + payment.getId())
                .execute()
                .map(SqlResult::size);
    }

    private Future<Integer> updatePaymentTo(SqlConnection sqlConnection, Payment payment, String status) {
        return sqlConnection.query("UPDATE PAYMENT SET STATUS = '" + status + "' WHERE ID = " + payment.getId())
                .execute()
                .map(SqlResult::size);
    }

    private Future<Payment> retrievePayment(Long id) {
        return pool.query("SELECT * FROM PAYMENT WHERE ID = " + id)
                .execute()
                .map(rows -> {
                    RowIterator<Row> iterator = rows.iterator();
                    return iterator.hasNext() ? iterator.next() : null;
                })
                .map(row -> {
                    if (null == row) {
                        return null;
                    }
                    return new Payment(row.getLong("ID"), row.getString("STATUS"), row.getOffsetDateTime("CREATE_TIME"));
                });
    }

    private Future<Row> retrieveTask(Long id) {
        return pool.query("SELECT * FROM TASKS WHERE ID = " + id)
                .execute()
                .map(rows -> {
                    RowIterator<Row> iterator = rows.iterator();
                    return iterator.hasNext() ? iterator.next() : null;
                });
    }

}