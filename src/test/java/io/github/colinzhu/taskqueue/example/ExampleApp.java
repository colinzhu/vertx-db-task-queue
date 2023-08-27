package io.github.colinzhu.taskqueue.example;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.h2.tools.Server;
import org.slf4j.LoggerFactory;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

@Slf4j
public class ExampleApp {
    @SneakyThrows
    public static void main(String[] args) {
        setLogLevel(ROOT_LOGGER_NAME, Level.INFO);
        setLogLevel("com.mchange.v2.resourcepool.BasicResourcePool", Level.INFO);
        setLogLevel("io.github.colinzhu.taskqueue", Level.INFO);
        setLogLevel("io.github.colinzhu.taskqueue.manager.TaskDao", Level.WARN);
        setLogLevel(PaymentCheckTaskProcessor.class.getName(), Level.INFO);

        Server.createTcpServer("-tcpPort", "9092", "-tcpAllowOthers", "-ifNotExists", "-baseDir", "./").start();
        log.info("H2 database TCP server created at default port (9092). jdbc:h2:tcp://localhost/example-db");
        Server.createWebServer("-webPort", "9091").start();
        log.info("H2 database WEB server created at 9091 port. http://localhost:9091/");

        Vertx vertx = Vertx.vertx();
        JDBCPool pool = getJdbcPool(vertx);
        Future f1 = createPaymentTable(pool);
        Future f2 = createTaskTable(pool);
        Future.join(f1, f2)
                .onSuccess(tablesCreated -> {
                    Verticle createVerticle = new PaymentCreateVerticle(pool);
                    vertx.deployVerticle(PaymentCheckVerticle.class, new DeploymentOptions().setInstances(2))
                            .compose(any -> vertx.deployVerticle(createVerticle))
                            .onFailure(err -> log.error("error", err));
                })
                .onFailure(err -> log.error("Unable to create tables", err));
    }

    public static JDBCPool getJdbcPool(Vertx vertx) {
        final JsonObject config = new JsonObject()
//                .put("url", "jdbc:h2:./example-db")
                .put("url", "jdbc:h2:tcp://localhost/example-db")
                .put("driver_class", "org.h2.Driver")
                .put("datasourceName", "example-db")
                .put("user", "sa")
                .put("password", "sa")
                .put("max_pool_size", 20);

        // prepare basic component
        return JDBCPool.pool(vertx, config);
    }

    private static Future<RowSet<Row>> createTaskTable(JDBCPool pool) {
        String sql = """
                drop table TASKS IF EXISTS;
                create table TASKS (
                    ID bigint auto_increment,
                    QUEUE_NAME varchar2(50) NOT NULL,
                    STATUS varchar2(30),
                    PAYLOAD CLOB,
                    REFERENCE_NUMBER varchar(100),
                    CREATE_TIME TIMESTAMP with TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    NEXT_PROCESS_TIME   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    LAST_UPDATE_TIME   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                """;
        return pool.query(sql).execute()
                .onSuccess(result -> log.info("TASK TABLE CREATED"))
                .onFailure(err -> log.error("Fail to create TASKS table", err));
    }


    private static Future<RowSet<Row>> createPaymentTable(JDBCPool pool) {
        String sql = """
                drop table payment IF EXISTS;
                create table PAYMENT (
                    ID bigint auto_increment,
                    STATUS varchar(30),
                    CREATE_TIME bigint,
                    INSTANCE varchar(30),
                    PRIMARY KEY (ID)
                );
                """;
        return pool.query(sql).execute()
                .onSuccess(result -> log.info("PAYMENT TABLE CREATED"))
                .onFailure(err -> log.error("Fail to create PAYMENT table", err));
    }

    private static void setLogLevel(String logger, Level level) {
        Logger taskQueueLogger = (Logger) LoggerFactory.getLogger(logger);
        taskQueueLogger.setLevel(level);
    }
}
