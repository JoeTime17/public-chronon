package ai.chronon.service.handlers;

import ai.chronon.api.Constants;
import ai.chronon.online.Api;
import ai.chronon.online.JavaStatsDriftResponse;
import ai.chronon.online.JavaStatsService;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;

/**
 * Handler for computing drift between two enhanced-stat percentile sketch windows.
 *
 * Endpoint: GET /v1/stats/:tableName/drift
 * Query parameters:
 *   - referenceStartTime: Reference window start time in milliseconds (required)
 *   - referenceEndTime: Reference window end time in milliseconds (required)
 *   - comparisonStartTime: Comparison window start time in milliseconds (required)
 *   - comparisonEndTime: Comparison window end time in milliseconds (required)
 *   - metric: Approx percentile metric name, or source column name (required when table has multiple percentile metrics)
 *   - dataset: Dataset name (optional, defaults to ENHANCED_STATS)
 *   - semanticHash: Config hash to read from a specific shard (optional)
 */
public class StatsDriftHandler implements Handler<RoutingContext> {

    private static final Logger logger = LoggerFactory.getLogger(StatsDriftHandler.class);
    private final Api api;
    private final ExecutionContext ec = ExecutionContext$.MODULE$.global();

    public StatsDriftHandler(Api api) {
        this.api = api;
    }

    /**
     * Parses explicit reference/comparison window parameters, delegates to JavaStatsService, and returns the
     * L-inf/L2/L1 distance object directly as JSON.
     */
    @Override
    public void handle(RoutingContext ctx) {
        String tableName = ctx.pathParam("tableName");
        if (tableName == null || tableName.trim().isEmpty()) {
            sendError(ctx, 400, "Missing required path parameter: tableName");
            return;
        }

        String referenceStartTimeStr = ctx.request().getParam("referenceStartTime");
        String referenceEndTimeStr = ctx.request().getParam("referenceEndTime");
        String comparisonStartTimeStr = ctx.request().getParam("comparisonStartTime");
        String comparisonEndTimeStr = ctx.request().getParam("comparisonEndTime");
        String metric = ctx.request().getParam("metric");
        String datasetName = ctx.request().getParam("dataset");
        String semanticHash = ctx.request().getParam("semanticHash");

        if (referenceStartTimeStr == null || referenceEndTimeStr == null ||
                comparisonStartTimeStr == null || comparisonEndTimeStr == null) {
            sendError(ctx, 400, "Missing required query parameters: referenceStartTime, referenceEndTime, comparisonStartTime, comparisonEndTime");
            return;
        }

        long referenceStartTimeMillis;
        long referenceEndTimeMillis;
        long comparisonStartTimeMillis;
        long comparisonEndTimeMillis;
        try {
            referenceStartTimeMillis = Long.parseLong(referenceStartTimeStr);
            referenceEndTimeMillis = Long.parseLong(referenceEndTimeStr);
            comparisonStartTimeMillis = Long.parseLong(comparisonStartTimeStr);
            comparisonEndTimeMillis = Long.parseLong(comparisonEndTimeStr);
        } catch (NumberFormatException e) {
            sendError(ctx, 400, "Invalid time parameters: all times must be valid long values");
            return;
        }

        if (referenceStartTimeMillis > referenceEndTimeMillis ||
                comparisonStartTimeMillis > comparisonEndTimeMillis) {
            sendError(ctx, 400, "Invalid time range: start times must be less than or equal to end times");
            return;
        }

        if (datasetName == null || datasetName.trim().isEmpty()) {
            datasetName = Constants.EnhancedStatsDataset();
        }

        logger.info("Computing stats drift for table: {}, metric: {}, referenceRange: [{}, {}], comparisonRange: [{}, {}], dataset: {}, semanticHash: {}",
                tableName, metric, referenceStartTimeMillis, referenceEndTimeMillis,
                comparisonStartTimeMillis, comparisonEndTimeMillis, datasetName,
                semanticHash != null ? semanticHash : "(none)");

        JavaStatsService statsService = new JavaStatsService(api, "ENHANCED_STATS", datasetName, ec);
        CompletableFuture<JavaStatsDriftResponse> driftResponseFuture =
                statsService.fetchDrift(tableName, referenceStartTimeMillis, referenceEndTimeMillis,
                        comparisonStartTimeMillis, comparisonEndTimeMillis, metric, semanticHash);

        Future<JavaStatsDriftResponse> vertxFuture = Future.fromCompletionStage(driftResponseFuture);

        vertxFuture.onSuccess(driftResponse -> {
            if (driftResponse.isSuccess()) {
                JsonObject responseJson = new JsonObject();
                driftResponse.getDistances().forEach(responseJson::put);

                ctx.response()
                        .setStatusCode(200)
                        .putHeader("content-type", "application/json")
                        .end(responseJson.encode());

                logger.info("Successfully computed drift for table: {}, metric: {}, referenceTiles: {}, comparisonTiles: {}",
                        tableName, driftResponse.getMetricName(), driftResponse.getReferenceTilesCount(),
                        driftResponse.getComparisonTilesCount());
            } else {
                sendError(ctx, 500, driftResponse.getErrorMessage());
            }
        });

        vertxFuture.onFailure(err -> {
            logger.error("Failed to compute drift for table: " + tableName, err);
            sendError(ctx, 500, "Failed to compute drift: " + err.getMessage());
        });
    }

    private void sendError(RoutingContext ctx, int statusCode, String message) {
        List<String> errors = Collections.singletonList(message);
        ctx.response()
                .setStatusCode(statusCode)
                .putHeader("content-type", "application/json")
                .end(new JsonObject().put("success", false).put("errors", errors).encode());
    }
}
