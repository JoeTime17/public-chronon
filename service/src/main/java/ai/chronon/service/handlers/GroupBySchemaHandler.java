package ai.chronon.service.handlers;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaGroupBySchemaResponse;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class GroupBySchemaHandler implements Handler<RoutingContext> {

    private final JavaFetcher fetcher;
    private static final Logger logger = LoggerFactory.getLogger(GroupBySchemaHandler.class);

    public GroupBySchemaHandler(JavaFetcher fetcher) {
        this.fetcher = fetcher;
    }

    @Override
    public void handle(RoutingContext ctx) {
        String entityName = ctx.pathParam("name");

        logger.debug("Retrieving groupBy schema for {}", entityName);

        ctx.vertx()
                .<JTry<JavaGroupBySchemaResponse>>executeBlocking(() -> fetcher.fetchGroupBySchema(entityName))
                .onSuccess(groupBySchemaResponseTry -> handleResult(ctx, entityName, groupBySchemaResponseTry))
                .onFailure(exception -> writeError(ctx, entityName, exception));
    }

    private void handleResult(RoutingContext ctx,
                              String entityName,
                              JTry<JavaGroupBySchemaResponse> groupBySchemaResponseTry) {
        if (!groupBySchemaResponseTry.isSuccess()) {
            writeError(ctx, entityName, groupBySchemaResponseTry.getException());
            return;
        }

        JavaGroupBySchemaResponse groupBySchemaResponse = groupBySchemaResponseTry.getValue();

        ctx.response()
                .setStatusCode(200)
                .putHeader("content-type", "application/json")
                .end(JsonObject.mapFrom(groupBySchemaResponse).encode());
    }

    private void writeError(RoutingContext ctx, String entityName, Throwable exception) {
        logger.error("Unable to retrieve groupBy schema for: {}", entityName, exception);

        List<String> errorMessages = Collections.singletonList(exception.getMessage());
        int statusCode = exception instanceof IllegalArgumentException ? 400 : 500;

        ctx.response()
                .setStatusCode(statusCode)
                .putHeader("content-type", "application/json")
                .end(new JsonObject().put("errors", errorMessages).encode());
    }
}
