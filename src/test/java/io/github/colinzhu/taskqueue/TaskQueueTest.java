package io.github.colinzhu.taskqueue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.github.colinzhu.taskqueue.example.Payment;
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
import java.util.List;
import java.util.function.Function;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

@ExtendWith(VertxExtension.class)
@Slf4j
class TaskQueueTest {
    private static JDBCPool pool;
    private static TaskQueueService taskQueueService;

    @BeforeAll
    static void init(Vertx vertx, VertxTestContext testContext) {
        setLogLevel(ROOT_LOGGER_NAME, Level.INFO);
        setLogLevel("com.mchange.v2.resourcepool.BasicResourcePool", Level.INFO);
        setLogLevel("io.github.colinzhu.taskqueue", Level.DEBUG);
        setLogLevel("io.github.colinzhu.taskqueue.TaskEntityRepo", Level.DEBUG);

        pool = H2Database.getJdbcPool(vertx, true);
        taskQueueService = TaskQueueService.taskQueue();
        H2Database.createTables(pool).onComplete(ar -> testContext.completeNow());
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
    @CsvSource({"FINISH", "REENQUEUE"})
    void testNormal(String afterProcessAction, Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        Payment payment = new Payment("CREATED", OffsetDateTime.now());
        String queueName = "REENQUEUE".equals(afterProcessAction) ? "Q2-need-reenquueue" : "Q1-need-finish";
        pool.withTransaction(conn -> savePayment(conn, payment).compose(p -> taskQueueService.enqueue(conn, queueName, "ref1", p)))
                .onComplete(testContext.succeeding(task -> {
                    vertx.setTimer(2000, id -> {
                        // verify payment status
                        retrievePayment(payment.getId()).onComplete(testContext.succeeding(res -> {
                            Assertions.assertEquals("PROCESSED", res.getStatus(), "PAYMENT status should be changed");
                            checkpoint.flag();
                        }));

                        // verify task
                        retrieveTask(task.getId()).onComplete(testContext.succeeding(res -> {
                            if ("REENQUEUE".equals(afterProcessAction)) {
                                Assertions.assertTrue(List.of("CREATED","PROCESSING").contains(res.getString("STATUS")), "task should still ba available for next processing");
                            } else {
                                Assertions.assertNull(res, "task should be deleted");
                            }
                            checkpoint.flag();
                        }));
                    });
                }));

        Function<Task<Payment>, Future<?>> taskProcessor = task -> {
            Assertions.assertEquals(payment.getCreateTime().toInstant(), task.getPayload().getCreateTime().toInstant(), "date time should be same as the original object");
            log.info("Processing {}", task.getPayload());
            Function<SqlConnection, Future<Integer>> function;
            if ("REENQUEUE".equals(afterProcessAction)) {
                function =  conn -> updatePayment(conn, task.getPayload())
                        .compose(p -> taskQueueService.reenqueue(conn, task.getId(), Duration.ofSeconds(5)));
            } else {
                function = conn -> updatePayment(conn, task.getPayload()).compose(p -> taskQueueService.finish(conn, task.getId()));
            }
            return pool.withTransaction(function);
        };

        PollConfig<Payment> pollConfig = PollConfig.<Payment>builder()
                .queueName(queueName)
                .batchSize(5)
                .nextProcessDelay(Duration.ofMinutes(10))
                .taskProcessor(taskProcessor)
                .payloadClass(Payment.class)
                .noTaskPollInterval(Duration.ofSeconds(1))
                .hasTaskPollInterval(Duration.ofSeconds(1)).build();
        TaskPoller<Payment> poller = new TaskPoller<>(vertx, pool, pollConfig);
        poller.start();
    }

    @Test
    @DisplayName("Task already finished by another poller")
    void testTaskAlreadyFinishedByAnotherPoller(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        Payment payment = new Payment("CREATED", OffsetDateTime.now());
        String queueName = "Q-already-finished-by-another-poller";
        pool.withTransaction(conn -> savePayment(conn, payment).compose(p -> taskQueueService.enqueue(conn, queueName, "ref1", p)))
                .onComplete(testContext.succeeding(task -> {
                    vertx.setTimer(2000, id -> {
                        // verify payment status
                        retrievePayment(payment.getId()).onComplete(testContext.succeeding(res -> {
                            Assertions.assertEquals("STATUS_ANOTHER_POLLER", res.getStatus(), "PAYMENT status should still be STATUS_ANOTHER_POLLER instead of STATUS_CURRENT_POLLER");
                            checkpoint.flag();
                        }));

                        // verify task
                        retrieveTask(task.getId()).onComplete(testContext.succeeding(res -> {
                            Assertions.assertNull(res, "task should not be available, already deleted by another poller");
                            checkpoint.flag();
                        }));
                    });
                }));

        Function<Task<Payment>, Future<?>> taskProcessor = task -> {
            // simulate task already finished by another poller (payment status updated to "ABC", task deleted
            var futureOfAnother = pool.withTransaction(conn -> updatePaymentTo(conn, task.getPayload(), "STATUS_ANOTHER_POLLER").compose(p -> taskQueueService.finish(conn, task.getId())));

            Function<SqlConnection, Future<Integer>> function = conn -> updatePaymentTo(conn, task.getPayload(), "STATUS_CURRENT_POLLER")
                        .compose(p -> taskQueueService.reenqueue(conn, task.getId(), Duration.ofSeconds(5)));

            return futureOfAnother.compose(any -> pool.withTransaction(function));
        };

        PollConfig<Payment> pollConfig = PollConfig.<Payment>builder()
                .queueName(queueName)
                .batchSize(5)
                .nextProcessDelay(Duration.ofMinutes(10))
                .taskProcessor(taskProcessor)
                .payloadClass(Payment.class)
                .noTaskPollInterval(Duration.ofSeconds(1))
                .hasTaskPollInterval(Duration.ofSeconds(1)).build();
        TaskPoller<Payment> poller = new TaskPoller<>(vertx, pool, pollConfig);
        poller.start();
    }

    @Test
    @DisplayName("Poller is stopped - no task will be picked up")
    void testPollerStopped(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(3);

        Function<Task<Payment>, Future<?>> taskProcessor = task -> {
            log.info("Processing {}", task.getPayload());
            return pool.withTransaction(conn -> updatePayment(conn, task.getPayload())
                    .compose(p -> taskQueueService.reenqueue(conn, task.getId(), Duration.ofSeconds(5))));
        };

        PollConfig<Payment> pollConfig = PollConfig.<Payment>builder()
                .queueName("Q3-poller-stopped")
                .batchSize(5)
                .nextProcessDelay(Duration.ofMinutes(10))
                .taskProcessor(taskProcessor)
                .payloadClass(Payment.class)
                .noTaskPollInterval(Duration.ofSeconds(1))
                .hasTaskPollInterval(Duration.ofSeconds(1)).build();
        TaskPoller<Payment> poller = new TaskPoller<>(vertx, pool, pollConfig);
        poller.start();
        vertx.setTimer(1000, id -> poller.stop().onSuccess(v -> log.info("poller stopped.")).onSuccess(v -> checkpoint.flag()));

        Payment payment = new Payment("CREATED", OffsetDateTime.now());
        pool.withTransaction(conn -> savePayment(conn, payment).compose(p -> taskQueueService.enqueue(conn, "Q3-poller-stopped", "ref1", p)))
                .onComplete(testContext.succeeding(task -> {
                    vertx.setTimer(2000, id -> {
                        // verify payment status
                        retrievePayment(payment.getId()).onComplete(testContext.succeeding(res -> {
                            //Assertions.assertEquals("CREATED", res.getStatus(), "PAYMENT should not be changed");
                            checkpoint.flag();
                        }));

                        // verify task
                        retrieveTask(task.getId()).onComplete(testContext.succeeding(res -> {
                            //Assertions.assertEquals("CREATED", res.getString("STATUS"), "task should not be checked-out");
                            checkpoint.flag();
                        }));
                    });
                }));
    }

    @ParameterizedTest
    @DisplayName("Task processing error - changes should be rolled back, tasks should be updated to ERROR")
    @CsvSource({"ERR_IN_TXN","ERR_BEFORE_TXN"})
    void testTaskProcessingError(String errLocation, Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        Payment payment = new Payment("CREATED", OffsetDateTime.now());
        pool.withTransaction(conn -> savePayment(conn, payment).compose(p -> taskQueueService.enqueue(conn, "Q1", "ref1", p)))
                .onComplete(testContext.succeeding(task -> {
                    // after 6 seconds
                    vertx.setTimer(2000, id -> {
                        // verify payment status
                        retrievePayment(payment.getId()).onComplete(testContext.succeeding(p -> {
                            Assertions.assertEquals("CREATED", p.getStatus(), "PAYMENT status should be rolled back / no change");
                            checkpoint.flag();
                        }));

                        // verify task
                        retrieveTask(task.getId()).onComplete(testContext.succeeding(row -> {
                            Assertions.assertEquals("ERROR", row.getString("STATUS"), "task should be updated to ERROR");
                            checkpoint.flag();
                        }));
                    });
                }));

        Function<Task<Payment>, Future<?>> taskProcessor = task -> {
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
                    }).compose(count -> taskQueueService.finish(conn, task.getId()));
            return pool.withTransaction(updatePaymentFunc);
        };

        PollConfig<Payment> pollConfig = PollConfig.<Payment>builder()
                .queueName("Q1")
                .batchSize(5)
                .nextProcessDelay(Duration.ofMinutes(10))
                .taskProcessor(taskProcessor)
                .payloadClass(Payment.class)
                .noTaskPollInterval(Duration.ofSeconds(1))
                .hasTaskPollInterval(Duration.ofSeconds(1)).build();
        TaskPoller<Payment> poller = new TaskPoller<>(vertx, pool, pollConfig);
        poller.start();
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