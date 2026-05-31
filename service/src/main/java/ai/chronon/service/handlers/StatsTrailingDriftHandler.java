package ai.chronon.service.handlers;

import ai.chronon.api.Constants;
import ai.chronon.online.Api;
import ai.chronon.online.JavaStatsService;
import ai.chronon.online.JavaStatsTrailingDriftPoint;
import ai.chronon.online.JavaStatsTrailingDriftResponse;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;

/**
 * Handler for computing daily trailing drift values over enhanced-stat percentile sketch windows.
 *
 * Endpoint: GET /v1/stats/:tableName/trailing-drift
 * Query parameters:
 *   - startDate: First anchor date, yyyy-MM-dd (required)
 *   - endDate: Last anchor date, yyyy-MM-dd (required)
 *   - windowDays: Trailing window size in days (required)
 *   - metric: Approx percentile metric name, or source column name (required when table has multiple percentile metrics)
 *   - distance: Distance to return as y: linf, l2, or l1 (optional, defaults to linf)
 *   - dataset: Dataset name (optional, defaults to ENHANCED_STATS)
 *   - semanticHash: Config hash to read from a specific shard (optional)
 */
public class StatsTrailingDriftHandler implements Handler<RoutingContext> {

    private static final Logger logger = LoggerFactory.getLogger(StatsTrailingDriftHandler.class);
    private final Api api;
    private final ExecutionContext ec = ExecutionContext$.MODULE$.global();

    public StatsTrailingDriftHandler(Api api) {
        this.api = api;
    }

    /**
     * Parses trailing-window request parameters, delegates daily drift generation to JavaStatsService, and returns
     * a JSON array of x/y points. Per-date failures are encoded on their individual points.
     */
    @Override
    public void handle(RoutingContext ctx) {
        String tableName = ctx.pathParam("tableName");
        if (tableName == null || tableName.trim().isEmpty()) {
            sendError(ctx, 400, "Missing required path parameter: tableName");
            return;
        }

        String startDate = ctx.request().getParam("startDate");
        String endDate = ctx.request().getParam("endDate");
        String windowDaysStr = ctx.request().getParam("windowDays");
        String metric = ctx.request().getParam("metric");
        String distance = ctx.request().getParam("distance");
        String datasetName = ctx.request().getParam("dataset");
        String semanticHash = ctx.request().getParam("semanticHash");

        if (startDate == null || endDate == null || windowDaysStr == null) {
            sendError(ctx, 400, "Missing required query parameters: startDate, endDate, windowDays");
            return;
        }

        int windowDays;
        try {
            windowDays = Integer.parseInt(windowDaysStr);
        } catch (NumberFormatException e) {
            sendError(ctx, 400, "Invalid windowDays parameter: windowDays must be a valid integer");
            return;
        }

        if (windowDays <= 0) {
            sendError(ctx, 400, "Invalid windowDays parameter: windowDays must be greater than 0");
            return;
        }

        try {
            LocalDate parsedStartDate = LocalDate.parse(startDate);
            LocalDate parsedEndDate = LocalDate.parse(endDate);
            if (parsedStartDate.isAfter(parsedEndDate)) {
                sendError(ctx, 400, "Invalid date range: startDate must be less than or equal to endDate");
                return;
            }
        } catch (DateTimeParseException e) {
            sendError(ctx, 400, "Invalid date parameters: startDate and endDate must use yyyy-MM-dd");
            return;
        }

        if (datasetName == null || datasetName.trim().isEmpty()) {
            datasetName = Constants.EnhancedStatsDataset();
        }

        logger.info("Computing trailing stats drift for table: {}, metric: {}, distance: {}, dateRange: [{}, {}], windowDays: {}, dataset: {}, semanticHash: {}",
                tableName, metric, distance, startDate, endDate, windowDays, datasetName,
                semanticHash != null ? semanticHash : "(none)");

        JavaStatsService statsService = new JavaStatsService(api, "ENHANCED_STATS", datasetName, ec);
        CompletableFuture<JavaStatsTrailingDriftResponse> driftResponseFuture =
                statsService.fetchTrailingDrift(tableName, startDate, endDate, windowDays, metric, distance, semanticHash);

        Future<JavaStatsTrailingDriftResponse> vertxFuture = Future.fromCompletionStage(driftResponseFuture);

        vertxFuture.onSuccess(driftResponse -> {
            if (driftResponse.isSuccess()) {
                JsonArray responseJson = new JsonArray();
                for (JavaStatsTrailingDriftPoint point : driftResponse.getPoints()) {
                    JsonObject pointJson = new JsonObject()
                            .put("x", point.getX())
                            .put("y", point.getY());
                    if (!point.isSuccess()) {
                        pointJson.put("error", point.getErrorMessage());
                    }
                    responseJson.add(pointJson);
                }

                ctx.response()
                        .setStatusCode(200)
                        .putHeader("content-type", "application/json")
                        .end(responseJson.encode());

                logger.info("Successfully computed trailing drift for table: {}, points: {}",
                        tableName, driftResponse.getPoints().size());
            } else {
                sendError(ctx, 500, driftResponse.getErrorMessage());
            }
        });

        vertxFuture.onFailure(err -> {
            logger.error("Failed to compute trailing drift for table: " + tableName, err);
            sendError(ctx, 500, "Failed to compute trailing drift: " + err.getMessage());
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
