package ai.chronon.service;

import ai.chronon.online.Api;
import ai.chronon.online.JavaFetcher;
import ai.chronon.service.handlers.FetchRouter;
import ai.chronon.service.handlers.FetchRouterV2;
import ai.chronon.service.handlers.GroupBySchemaHandler;
import ai.chronon.service.handlers.GroupByStatusHandler;
import ai.chronon.service.handlers.JoinListHandler;
import ai.chronon.service.handlers.JoinSchemaHandler;
import ai.chronon.service.handlers.StatsDriftHandler;
import ai.chronon.service.handlers.StatsHandler;
import ai.chronon.service.handlers.StatsTrailingDriftHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.Http2Settings;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the Chronon fetcher endpoints. We wire up our API routes and configure and launch our HTTP service here.
 * We choose to use just 1 verticle for now as it allows us to keep things simple and we don't need to scale /
 * independently deploy different endpoint routes.
 */
public class FetcherVerticle extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(FetcherVerticle.class);

    private HttpServer server;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        ConfigStore cfgStore = new ConfigStore(vertx);

        Api api = ApiProvider.buildApi(cfgStore);

        long joinConfTtl = cfgStore.getJoinConfTtlMillis();
        long joinCodecTtl = cfgStore.getJoinCodecTtlMillis();
        logger.info("Join conf TTL: {}ms, Join codec TTL: {}ms", joinConfTtl, joinCodecTtl);

        // Execute the blocking Bigtable initialization in a separate worker thread
        vertx.executeBlocking(() -> api.buildJavaFetcher("feature-service", false, joinConfTtl, joinCodecTtl))
        .onSuccess(fetcher -> {
            try {
                // This code runs back on the event loop when the blocking operation completes
                startHttpServer(cfgStore.getServerPort(), cfgStore.encodeConfig(), api, fetcher, startPromise);
            } catch (Exception e) {
                startPromise.fail(e);
            }
        })
        .onFailure(startPromise::fail);
    }

    protected void startHttpServer(int port, String configJsonString, Api api, JavaFetcher fetcher, Promise<Void> startPromise) throws Exception {
        Router router = Router.router(vertx);

        // Define routes

        // Set up sub-routes for the various feature retrieval apis
        router.route("/v1/fetch/*").subRouter(FetchRouter.createFetchRoutes(vertx, fetcher));
        router.route("/v2/fetch/*").subRouter(FetchRouterV2.createFetchRoutes(vertx, fetcher));

        // Set up route for list of online joins
        router.get("/v1/joins").handler(new JoinListHandler(fetcher));

        // Set up route for retrieval of Join schema
        router.get("/v1/join/:name/schema").handler(new JoinSchemaHandler(fetcher));

        // Set up route for retrieval of GroupBy schema
        router.get("/v1/groupby/:name/schema").handler(new GroupBySchemaHandler(fetcher));

        // Set up route for retrieval of GroupBy status
        router.get("/v1/groupby/:name/status").handler(new GroupByStatusHandler(fetcher));

        // Set up route for fetching enhanced statistics
        router.get("/v1/stats/:tableName/trailing-drift").handler(new StatsTrailingDriftHandler(api));
        router.get("/v1/stats/:tableName/drift").handler(new StatsDriftHandler(api));
        router.get("/v1/stats/:tableName").handler(new StatsHandler(api));

        // Health check route
        router.get("/ping").handler(ctx -> {
            ctx.json("Pong!");
        });

        // Add route to show current configuration
        router.get("/config").handler(ctx -> {
            ctx.response()
               .putHeader("content-type", "application/json")
               .end(configJsonString);
        });

        Http2Settings http2Settings = new Http2Settings()
                .setMaxConcurrentStreams(200)
                .setInitialWindowSize(1024 * 1024);

        // Start HTTP server
        HttpServerOptions httpOptions =
                new HttpServerOptions()
                        .setCompressionSupported(true)
                        .setTcpKeepAlive(true)
                        .setIdleTimeout(60)
                        // HTTP/2 specific settings - these are currently the default in our Vert.x version
                        // but we make them explicit for clarity and to guard against future changes
                        .setUseAlpn(false)           // No need for ALPN in cleartext mode
                        .setSsl(false)               // No SSL for cleartext
                        .setHttp2ClearTextEnabled(true)
                        .setInitialSettings(http2Settings);
        server = vertx.createHttpServer(httpOptions);
        server.requestHandler(router)
                .listen(port)
                .onSuccess(server -> {
                    logger.info("HTTP server started on port {}", server.actualPort());
                    startPromise.complete();
                })
                .onFailure(err -> {
                    logger.error("Failed to start HTTP server", err);
                    startPromise.fail(err);
                });
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        logger.info("Stopping HTTP server...");
        if (server != null) {
            server.close()
                    .onSuccess(v -> {
                        logger.info("HTTP server stopped successfully");
                        stopPromise.complete();
                    })
                    .onFailure(err -> {
                        logger.error("Failed to stop HTTP server", err);
                        stopPromise.fail(err);
                    });
        } else {
            stopPromise.complete();
        }
    }
}
