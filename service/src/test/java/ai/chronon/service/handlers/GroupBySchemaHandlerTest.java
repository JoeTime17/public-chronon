package ai.chronon.service.handlers;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaGroupBySchemaResponse;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.RoutingContext;
import org.apache.avro.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

@RunWith(VertxUnitRunner.class)
public class GroupBySchemaHandlerTest {
    @Mock
    private JavaFetcher mockFetcher;

    @Mock
    private RoutingContext routingContext;

    @Mock
    private HttpServerResponse response;

    private GroupBySchemaHandler handler;
    private Vertx vertx;
    private AutoCloseable mocksCloseable;

    @Before
    public void setUp(TestContext context) {
        mocksCloseable = MockitoAnnotations.openMocks(this);
        vertx = Vertx.vertx();

        handler = new GroupBySchemaHandler(mockFetcher);

        when(routingContext.vertx()).thenReturn(vertx);
        when(routingContext.response()).thenReturn(response);
        when(response.putHeader(anyString(), anyString())).thenReturn(response);
        when(response.setStatusCode(anyInt())).thenReturn(response);
        when(routingContext.pathParam("name")).thenReturn("test_group_by");
    }

    @After
    public void tearDown() throws Exception {
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        if (mocksCloseable != null) {
            mocksCloseable.close();
        }
    }

    @Test
    public void testSuccessfulRequest(TestContext context) {
        Async async = context.async();

        String keySchema = "{\"type\":\"record\",\"name\":\"Key\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
        String valueSchema = "{\"type\":\"record\",\"name\":\"Value\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"score\",\"type\":[\"null\",\"long\"]}]}";
        String inputSchema = "{\"type\":\"record\",\"name\":\"Input\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"raw_score\",\"type\":\"long\"}]}";
        String selectedSchema = "{\"type\":\"record\",\"name\":\"Selected\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"score\",\"type\":\"long\"}]}";
        JavaGroupBySchemaResponse groupBySchemaResponse =
                new JavaGroupBySchemaResponse("test_group_by", keySchema, valueSchema, inputSchema, selectedSchema);
        JTry<JavaGroupBySchemaResponse> groupBySchemaResponseTry = JTry.success(groupBySchemaResponse);

        when(mockFetcher.fetchGroupBySchema(anyString())).thenReturn(groupBySchemaResponseTry);

        verifyResponseOnEnd(context, async, 200, actualResponse -> {
            verify(mockFetcher).fetchGroupBySchema("test_group_by");

            context.assertEquals(actualResponse.getString("groupByName"), "test_group_by");
            context.assertEquals(actualResponse.getString("keySchema"), keySchema);
            context.assertEquals(actualResponse.getString("valueSchema"), valueSchema);
            context.assertEquals(actualResponse.getString("inputSchema"), inputSchema);
            context.assertEquals(actualResponse.getString("selectedSchema"), selectedSchema);

            new Schema.Parser().parse(keySchema);
            new Schema.Parser().parse(valueSchema);
            new Schema.Parser().parse(inputSchema);
            new Schema.Parser().parse(selectedSchema);
        });

        handler.handle(routingContext);
    }

    @Test
    public void testOfflineGroupByRequest(TestContext context) {
        Async async = context.async();

        String errorMessage = "GroupBy test_group_by is not online. Fetcher schema is only available for online GroupBys. " +
                "Use the Iceberg catalog schema via eval for the offline table schema, or enable online=True and upload the GroupBy.";
        when(mockFetcher.fetchGroupBySchema(anyString()))
                .thenReturn(JTry.failure(new IllegalArgumentException(errorMessage)));

        verifyResponseOnEnd(context, async, 400, actualResponse -> {
            verify(mockFetcher).fetchGroupBySchema("test_group_by");

            String failureString = actualResponse.getJsonArray("errors").getString(0);
            context.assertTrue(failureString.contains("Iceberg catalog schema via eval"));
            context.assertTrue(failureString.contains("online=True"));
        });

        handler.handle(routingContext);
    }

    @Test
    public void testFailedRequest(TestContext context) {
        Async async = context.async();

        when(mockFetcher.fetchGroupBySchema(anyString()))
                .thenReturn(JTry.failure(new RuntimeException("some fake failure")));

        verifyResponseOnEnd(context, async, 500, actualResponse -> {
            verify(mockFetcher).fetchGroupBySchema("test_group_by");

            context.assertTrue(actualResponse.containsKey("errors"));
            context.assertEquals(actualResponse.getJsonArray("errors").getString(0), "some fake failure");
        });

        handler.handle(routingContext);
    }

    private void verifyResponseOnEnd(TestContext context,
                                     Async async,
                                     int expectedStatusCode,
                                     ResponseVerifier verifier) {
        doAnswer(invocation -> {
            try {
                context.verify(v -> {
                    verify(response).setStatusCode(expectedStatusCode);
                    verify(response).putHeader("content-type", "application/json");
                    verifier.verify(new JsonObject((String) invocation.getArgument(0)));
                });
            } finally {
                async.complete();
            }
            return null;
        }).when(response).end(anyString());
    }

    private interface ResponseVerifier {
        void verify(JsonObject response);
    }
}
