package io.github.colinzhu.taskqueue.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.colinzhu.taskqueue.H2Database;
import io.github.colinzhu.taskqueue.TaskQueueSupportService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.jdbcclient.JDBCPool;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
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
        router.route().handler(BodyHandler.create());
        router.route("/support/taskqueue/reprocess").handler(this::reprocess);
        router.route("/support/taskqueue/search/:queueName/:status").handler(this::search);
        router.route("/support/taskqueue/count").handler(this::count);

        String logMsg = """
task queue support server started.
http://localhost:#{port}/support/taskqueue/reprocess
http://localhost:#{port}/support/taskqueue/search/payment.check/CREATED?size=5
http://localhost:#{port}/support/taskqueue/count
                """;

        server.requestHandler(router).listen(0)
                .onSuccess(httpServer -> log.info(logMsg.replace("#{port}", String.valueOf(httpServer.actualPort()))))
                .onFailure(err -> log.error("failed to start task queue support.", err));
    }

    private void reprocess(RoutingContext routingContext) {
        Set<Long> idList = routingContext.body().asJsonArray().stream().map(Object::toString).map(Long::valueOf).collect(Collectors.toSet());
        pool.withConnection(conn -> taskQueueSupportService.reprocessErrorTasks(conn, idList))
                .onSuccess(res -> routingContext.response().end("number of tasks reenqueued:" + res))
                .onFailure(err -> routingContext.response().end().failed());
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
