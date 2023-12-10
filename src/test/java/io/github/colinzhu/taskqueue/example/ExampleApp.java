package io.github.colinzhu.taskqueue.example;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.github.colinzhu.taskqueue.*;
import io.github.colinzhu.taskqueue.example.check.PaymentCheckTaskProcessor;
import io.github.colinzhu.taskqueue.example.release.PaymentReleaseTaskProcessor;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxInfluxDbOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

@Slf4j
public class ExampleApp {
    @SneakyThrows
    public static void main(String[] args) {
        setLogLevel(ROOT_LOGGER_NAME, Level.INFO);
        setLogLevel("com.mchange.v2.resourcepool.BasicResourcePool", Level.DEBUG);
        setLogLevel("io.github.colinzhu.taskqueue", Level.DEBUG);
        setLogLevel("io.github.colinzhu.taskqueue.TaskPoller", Level.DEBUG);
        setLogLevel("io.github.colinzhu.taskqueue.TaskQueueRepo", Level.DEBUG);
        setLogLevel("io.micrometer", Level.OFF);

        MicrometerMetricsOptions options = new MicrometerMetricsOptions()
                .setInfluxDbOptions(new VertxInfluxDbOptions().setEnabled(true))
                .setEnabled(true);

        Vertx vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(options));
        MeterRegistry registry = BackendRegistries.getDefaultNow();

        new ClassLoaderMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);


        Database db = Database.get(Database.H2);
        db.startServer();

        JDBCPool pool = db.getJdbcPool(vertx);
        db.createTables(pool)
                .onSuccess(tablesCreated -> deployVerticles(vertx, pool))
                .onFailure(err -> log.error("Unable to create tables", err));

        appendShutdownHook(vertx);
    }

    private static void deployVerticles(Vertx vertx, JDBCPool pool) {
        Supplier<JDBCPool> poolSupplier = () -> Database.get(Database.H2).getJdbcPool(vertx);
        TaskPollerConfig<Payment> taskPollerCheckConfig = new TaskPollerConfig<>("payment.check", Payment.class).setBatchSize(20);
        TaskPollerConfig<Payment> taskPollerReleaseConfig = new TaskPollerConfig<>("payment.release", Payment.class).setBatchSize(20);

        vertx.deployVerticle(new TaskProcessorVerticle<>(taskPollerCheckConfig.getQueueName(), () -> new PaymentCheckTaskProcessor(vertx, poolSupplier, TaskQueueService.taskQueue(vertx))))
                .compose(any -> vertx.deployVerticle(new TaskProcessorVerticle<>(taskPollerReleaseConfig.getQueueName(), () -> new PaymentReleaseTaskProcessor(vertx, poolSupplier, TaskQueueService.taskQueue(vertx)))))
                .compose(any -> vertx.deployVerticle(new TaskPollerVerticle<>(poolSupplier, taskPollerCheckConfig)))
                .compose(any -> vertx.deployVerticle(new TaskPollerVerticle<>(poolSupplier, taskPollerReleaseConfig)))
                .compose(any -> vertx.deployVerticle(() -> new WebVerticle(pool), new DeploymentOptions().setInstances(2)))
                .onFailure(err -> log.error("error", err));
    }

    private static void appendShutdownHook(Vertx vertx) {
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    final AtomicBoolean stopCompleted = new AtomicBoolean(false);
                    vertx.close(ar -> stopCompleted.set(true));

                    int maxSeconds = 20;
                    int seconds = 0;
                    while (!stopCompleted.get()) {
                        seconds++;
                        try {
                            Thread.sleep(Duration.ofSeconds(1).toMillis());
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        if (seconds >= maxSeconds) {
                            System.out.println("Reached maxSeconds=" + maxSeconds + ", stopped immediately");
                            break;
                        }
                    }
                    System.out.println("Successfully stopped vertx");
                })
        );
    }

    private static void setLogLevel(String logger, Level level) {
        Logger taskQueueLogger = (Logger) LoggerFactory.getLogger(logger);
        taskQueueLogger.setLevel(level);
    }
}
