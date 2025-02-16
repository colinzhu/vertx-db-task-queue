package io.github.colinzhu.taskqueue.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.colinzhu.taskqueue.internal.EventAddress;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.FileSystemAccess;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.jdbcclient.JDBCPool;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class TaskQueueSupportHandler implements Supplier<Router> {
    private final Vertx vertx;
    private final JDBCPool pool;
    private final TaskQueueSupportService taskQueueSupportService;

    @Override
    public Router get() {
        ObjectMapper mapper = DatabindCodec.mapper();
        mapper.registerModule(new JavaTimeModule()).disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        Router router = Router.router(vertx);
        router.route("/support/web/*").handler(StaticHandler.create(FileSystemAccess.ROOT, "io/github/colinzhu/taskqueue/web").setCachingEnabled(false));
        router.route().handler(BodyHandler.create());
        router.route("/support/api/reenqueue").handler(this::reenqueue);
        router.route("/support/api/mark-poison").handler(this::markPoison);
        router.route("/support/api/poison-to-error").handler(this::poisonToError);
        router.route("/support/api/housekeep-poison").handler(this::housekeepPoison);
        router.route("/support/api/search/:queueName/:status").handler(this::search);
        router.route("/support/api/count").handler(this::count);
        router.route("/support/api/poller-pause/:queueName").handler(this::pollerPause);
        router.route("/support/api/poller-start/:queueName").handler(this::pollerStart);
        return router;
    }

    private void reenqueue(RoutingContext routingContext) {
        log.info("reenqueue request body:{}", routingContext.body().asString());
        Future.succeededFuture()
                .map(any -> routingContext.body().asJsonArray().stream().map(Object::toString).map(Long::valueOf).collect(Collectors.toSet()))
                .compose(idList -> pool.withConnection(conn -> taskQueueSupportService.reenqueueFromError(conn, idList)))
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

    private void pollerPause(RoutingContext routingContext) {
        String queueName = routingContext.pathParam("queueName");
        pausePoller(vertx, queueName);
        routingContext.response().end(Json.encode("paused request sent for: " + queueName));
    }

    private void pollerStart(RoutingContext routingContext) {
        String queueName = routingContext.pathParam("queueName");
        startPoller(vertx, queueName);
        routingContext.response().end(Json.encode("start request sent for: " + queueName));
    }


    static void pausePoller(Vertx vertx, String queueName) {
        vertx.eventBus().publish(EventAddress.POLLER_PAUSE_PREFIX + queueName, null);
    }

    static void startPoller(Vertx vertx, String queueName) {
        vertx.eventBus().publish(EventAddress.POLLER_START_PREFIX + queueName, null);
    }

}
