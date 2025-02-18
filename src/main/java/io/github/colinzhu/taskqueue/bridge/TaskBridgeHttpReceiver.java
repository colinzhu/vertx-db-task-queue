package io.github.colinzhu.taskqueue.bridge;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.github.colinzhu.taskqueue.enqueue.TaskEnqueueService;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import io.vertx.jdbcclient.JDBCPool;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public class TaskBridgeHttpReceiver implements Handler<RoutingContext> {
    private final JDBCPool pool;
    private final TaskEnqueueService taskEnqueueService;
    @Setter
    @Accessors(fluent = true)
    private Function<String, String> queueNameMapper = queueName -> queueName;

    @Override
    public void handle(RoutingContext routingContext) {
        log.info("Task received:{}", routingContext.body().asString());
        Future.succeededFuture()
                .map(any -> routingContext.body().asPojo(TaskDto.class))
                .compose(obj -> pool.withConnection(conn -> taskEnqueueService.enqueue(conn, queueNameMapper.apply(obj.queueName), obj.refNumber, obj.payload)).map(obj))
                .onSuccess(task -> routingContext.response().end(Json.encode(Map.of("id", task.id, "refNumber", task.refNumber))))
                .onFailure(err -> {
                    log.error("Error handling task", err);
                    routingContext.response().setStatusCode(500).end(Json.encode(Map.of("reason", err.getMessage())));
                });
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class TaskDto {
        private String id;
        private String queueName;
        private String refNumber;
        private String payload;
    }

}
