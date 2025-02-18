package io.github.colinzhu.taskqueue.example;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.github.colinzhu.taskqueue.Database;
import io.github.colinzhu.taskqueue.bridge.TaskBridgeHttpSender;
import io.github.colinzhu.taskqueue.dispatch.TaskDispatchConfig;
import io.github.colinzhu.taskqueue.dispatch.TaskDispatchVerticle;
import io.github.colinzhu.taskqueue.enqueue.TaskEnqueueService;
import io.github.colinzhu.taskqueue.example.check.PaymentCheckTaskProcessor;
import io.github.colinzhu.taskqueue.example.release.PaymentReleaseTaskProcessor;
import io.github.colinzhu.taskqueue.process.TaskProcessService;
import io.github.colinzhu.taskqueue.process.TaskProcessVerticle;
import io.github.colinzhu.taskqueue.process.TaskProcessor;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.client.WebClient;
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
        setLogLevel("io.github.colinzhu.taskqueue.dispatch.TaskDispatcher", Level.DEBUG);
        setLogLevel("io.github.colinzhu.taskqueue.internal.TaskRepo", Level.DEBUG);
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

        TaskDispatchConfig<Payment> checkConfig = new TaskDispatchConfig<>("payment.check", Payment.class).setBatchSize(20);
        deploy(vertx, pool, () -> new PaymentCheckTaskProcessor(vertx, poolSupplier, TaskEnqueueService.getInstance(vertx), TaskProcessService.getInstance()), checkConfig);

        TaskDispatchConfig<String> releaseConfig = new TaskDispatchConfig<>("payment.release", String.class).setBatchSize(20);
        deploy(vertx, pool, () -> new TaskBridgeHttpSender(WebClient.create(vertx), "http://127.0.0.1:8080/taskqueue/bridge/receive", pool), releaseConfig);

        // PaymentReleaseTaskProcessor will process the task from TaskBridgeHttpReceiver
        TaskDispatchConfig<Payment> releaseRemoteConfig = new TaskDispatchConfig<>("payment.release.remote", Payment.class).setBatchSize(20);
        deploy(vertx, pool, () -> new PaymentReleaseTaskProcessor(vertx, poolSupplier, TaskProcessService.getInstance()), releaseRemoteConfig);

        vertx.deployVerticle(() -> new WebVerticle(pool), new DeploymentOptions().setInstances(2))
                .onSuccess(any -> log.info("Successfully deployed WebVerticle"))
                .onFailure(err -> log.error("error", err));
    }

    private static <T> void deploy(Vertx vertx, JDBCPool pool, Supplier<TaskProcessor<T>> taskProcessorSupplier, TaskDispatchConfig<T> config) {
        vertx.deployVerticle(new TaskProcessVerticle<>(config.getQueueName(), taskProcessorSupplier))
                .compose(any -> vertx.deployVerticle(new TaskDispatchVerticle<>(() -> pool, config)))
                .onSuccess(any -> log.info("Successfully deployed verticles for {}", config.getQueueName()))
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
                            log.info("Reached maxSeconds={}, stopped immediately", maxSeconds);
                            break;
                        }
                    }
                    log.info("Successfully stopped vertx");
                })
        );
    }

    private static void setLogLevel(String logger, Level level) {
        Logger taskQueueLogger = (Logger) LoggerFactory.getLogger(logger);
        taskQueueLogger.setLevel(level);
    }
}
