package ai.chronon.service.handlers;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaGroupByStatusResponse;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.RoutingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class GroupByStatusHandlerTest {
    @Mock
    private JavaFetcher mockFetcher;

    @Mock
    private RoutingContext routingContext;

    @Mock
    private HttpServerResponse response;

    private GroupByStatusHandler handler;
    private Vertx vertx;
    private AutoCloseable mocksCloseable;

    @Before
    public void setUp(TestContext context) {
        mocksCloseable = MockitoAnnotations.openMocks(this);
        vertx = Vertx.vertx();

        handler = new GroupByStatusHandler(mockFetcher);

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

        JavaGroupByStatusResponse groupByStatusResponse =
                new JavaGroupByStatusResponse("test_group_by", "2026-05-20");
        JTry<JavaGroupByStatusResponse> groupByStatusResponseTry = JTry.success(groupByStatusResponse);

        when(mockFetcher.fetchGroupByStatus(anyString())).thenReturn(groupByStatusResponseTry);

        verifyResponseOnEnd(context, async, 200, actualResponse -> {
            verify(mockFetcher).fetchGroupByStatus("test_group_by");

            context.assertEquals(actualResponse.getString("groupByName"), "test_group_by");
            context.assertEquals(actualResponse.getString("batchEndDate"), "2026-05-20");
        });

        handler.handle(routingContext);
    }

    @Test
    public void testOfflineGroupByRequest(TestContext context) {
        Async async = context.async();

        String errorMessage = "GroupBy test_group_by is not online. Fetcher status is only available for online GroupBys. " +
                "Enable online=True and upload the GroupBy.";
        when(mockFetcher.fetchGroupByStatus(anyString()))
                .thenReturn(JTry.failure(new IllegalArgumentException(errorMessage)));

        verifyResponseOnEnd(context, async, 400, actualResponse -> {
            verify(mockFetcher).fetchGroupByStatus("test_group_by");

            String failureString = actualResponse.getJsonArray("errors").getString(0);
            context.assertTrue(failureString.contains("online=True"));
            context.assertTrue(failureString.contains("upload the GroupBy"));
        });

        handler.handle(routingContext);
    }

    @Test
    public void testFailedRequest(TestContext context) {
        Async async = context.async();

        when(mockFetcher.fetchGroupByStatus(anyString()))
                .thenReturn(JTry.failure(new RuntimeException("some fake failure")));

        verifyResponseOnEnd(context, async, 500, actualResponse -> {
            verify(mockFetcher).fetchGroupByStatus("test_group_by");

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
