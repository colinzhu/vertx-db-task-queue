package io.github.colinzhu.taskqueue.bridge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.colinzhu.taskqueue.enqueue.TaskEnqueueService;
import io.github.colinzhu.taskqueue.process.Task;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.jdbcclient.JDBCPool;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class TaskHttpReceiveHandler implements Supplier<Router> {
    private final Vertx vertx;
    private final JDBCPool pool;
    private final TaskEnqueueService taskEnqueueService;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule()).disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Override
    public Router get() {
        ObjectMapper mapper = DatabindCodec.mapper();
        mapper.registerModule(new JavaTimeModule()).disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.route("/bridge/receive").handler(this::receiveTask);
        return router;
    }

    private void receiveTask(RoutingContext routingContext) {
        log.info("received task:{}", routingContext.body().asString());
        Future.succeededFuture()
                .map(any -> routingContext.body().asJsonObject())
                .map(jsonObj -> {
                            String jsonString = Json.encode(jsonObj);
                            log.info("JSON String: {}", jsonString);
                            return new Task<>(jsonObj.getLong("id"),
                                    jsonObj.getString("queueName"),
                                    jsonObj.getString("refNumber"),
                                    jsonObj.getInteger("attempt"),
                                    Json.encode(jsonObj.getJsonObject("payload")));
                        }

                )
                .compose(task -> pool.withConnection(conn -> taskEnqueueService.enqueue(conn, task.getQueueName() + ".received", task.getRefNumber(), task.getPayload())).map(task))
                .onSuccess(task -> routingContext.response().end(Json.encode(Map.of("id", task.getId(), "refNumber", task.getRefNumber()))))
                .onFailure(err -> {
                    log.error("receive task error.", err);
                    routingContext.response().setStatusCode(500).end(Json.encode(Map.of("reason", err.getMessage())));
                });
    }

}
