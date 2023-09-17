package io.github.colinzhu.taskqueue.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.colinzhu.taskqueue.H2Database;
import io.github.colinzhu.taskqueue.TaskQueueSupportService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.FileSystemAccess;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.jdbcclient.JDBCPool;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class TaskSupportVerticle extends AbstractVerticle {
    private JDBCPool pool;
    private final TaskQueueSupportService taskQueueSupportService = TaskQueueSupportService.getInstance();

    @Override
    public void start() {
        pool = H2Database.getJdbcPool(vertx, false);
        ObjectMapper mapper = DatabindCodec.mapper();
        mapper.registerModule(new JavaTimeModule()).disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        startHttpServer();
    }

    private void startHttpServer() {
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);
        router.route("/support/taskqueue/web/*").handler(StaticHandler.create(FileSystemAccess.ROOT, "/home/colin/dev/git/vertx-db-task-queue/src/test/resources/web").setCachingEnabled(false));
        router.route().handler(BodyHandler.create());
        router.route("/support/taskqueue/api/reenqueue").handler(this::reenqueue);
        router.route("/support/taskqueue/api/mark-poison").handler(this::markPoison);
        router.route("/support/taskqueue/api/poison-to-error").handler(this::poisonToError);
        router.route("/support/taskqueue/api/housekeep-poison").handler(this::housekeepPoison);
        router.route("/support/taskqueue/api/search/:queueName/:status").handler(this::search);
        router.route("/support/taskqueue/api/count").handler(this::count);

        String logMsg = """
                task queue support server started.
                http://localhost:#{port}/support/taskqueue/api/reenqueue
                http://localhost:#{port}/support/taskqueue/api/mark-poison
                http://localhost:#{port}/support/taskqueue/api/poison-to-error
                http://localhost:#{port}/support/taskqueue/api/housekeep-poison
                http://localhost:#{port}/support/taskqueue/api/search/payment.check/CREATED?size=5
                http://localhost:#{port}/support/taskqueue/api/count
                http://localhost:#{port}/support/taskqueue/web/
                                """;

        server.requestHandler(router).listen(31111)
                .onSuccess(httpServer -> log.info(logMsg.replace("#{port}", String.valueOf(httpServer.actualPort()))))
                .onFailure(err -> log.error("failed to start task queue support.", err));
    }

    private void reenqueue(RoutingContext routingContext) {
        log.info("reenqueue request body:{}", routingContext.body().asString());
        Future.succeededFuture()
                .map(any -> routingContext.body().asJsonArray().stream().map(Object::toString).map(Long::valueOf).collect(Collectors.toSet()))
                .compose(idList -> pool.withConnection(conn -> taskQueueSupportService.reenqueueErrorTasks(conn, idList)))
                .onSuccess(res -> routingContext.response().end(Json.encode(Map.of("count", res))))
                .onFailure(err -> routingContext.response().setStatusCode(500).end(Json.encode(Map.of("reason", "error"))));
    }

    private void markPoison(RoutingContext routingContext) {
        log.info("markPoison request body:{}", routingContext.body().asString());
        Future.succeededFuture()
                .map(any -> routingContext.body().asJsonArray().stream().map(Object::toString).map(Long::valueOf).collect(Collectors.toSet()))
                .compose(idList -> pool.withConnection(conn -> taskQueueSupportService.markPoison(conn, idList)))
                .onSuccess(res -> routingContext.response().end(Json.encode(Map.of("count", res))))
                .onFailure(err -> routingContext.response().setStatusCode(500).end(Json.encode(Map.of("reason", "error"))));
    }

    private void poisonToError(RoutingContext routingContext) {
        log.info("poisonToError request body:{}", routingContext.body().asString());
        Future.succeededFuture()
                .map(any -> routingContext.body().asJsonArray().stream().map(Object::toString).map(Long::valueOf).collect(Collectors.toSet()))
                .compose(idList -> pool.withConnection(conn -> taskQueueSupportService.poisonToError(conn, idList)))
                .onSuccess(res -> routingContext.response().end(Json.encode(Map.of("count", res))))
                .onFailure(err -> routingContext.response().setStatusCode(500).end(Json.encode(Map.of("reason", "error"))));
    }

    private void housekeepPoison(RoutingContext routingContext) {
        log.info("housekeepPoison request body:{}", routingContext.body().asString());
        OffsetDateTime createTimeBefore = OffsetDateTime.now().minus(Duration.ofDays(14));
        Future.succeededFuture()
                .map(any -> routingContext.body().asJsonArray().stream().map(Object::toString).map(Long::valueOf).collect(Collectors.toSet()))
                .compose(idList -> pool.withConnection(conn -> taskQueueSupportService.housekeepPoison(conn, idList, createTimeBefore)))
                .onSuccess(res -> routingContext.response().end(Json.encode(Map.of("count", res))))
                .onFailure(err -> routingContext.response().setStatusCode(500).end(Json.encode(Map.of("reason", "error"))));
    }

    private void search(RoutingContext routingContext) {
        String queueName = routingContext.pathParam("queueName");
        String status = routingContext.pathParam("status");
        List<String> sizeParams = routingContext.queryParam("size");
        int size;
        try {
            size = sizeParams.isEmpty() ? 100 : Integer.parseInt(sizeParams.get(0));
        } catch (Throwable e) {
            log.error("invalid param: size={}", sizeParams);
            routingContext.fail(400, e);
            return;
        }
        pool.withConnection(conn -> taskQueueSupportService.searchByQueueNameAndStatus(conn, queueName, status, size))
                .onSuccess(res -> routingContext.response().end(Json.encode(res)))
                .onFailure(err -> routingContext.fail(500, err));
    }

    private void count(RoutingContext routingContext) {
        pool.withConnection(taskQueueSupportService::countGroupByQueueNameAndStatus)
                .onSuccess(res -> routingContext.response().end(Json.encode(res)))
                .onFailure(err -> routingContext.fail(500, err));
    }

}
