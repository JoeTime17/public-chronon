/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online;

import ai.chronon.api.ScalaJavaConversions;
import ai.chronon.online.fetcher.Fetcher;
import ai.chronon.online.fetcher.FeaturesResponseType;
import ai.chronon.online.fetcher.FetcherResponseWithTs;
import scala.collection.Iterator;
import scala.Option;
import scala.collection.mutable.ArrayBuffer;
import scala.compat.java8.FutureConverters;
import scala.concurrent.Future;
import scala.concurrent.ExecutionContext;
import scala.util.Try;
import ai.chronon.online.metrics.Metrics;
import ai.chronon.online.metrics.TTLCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class JavaFetcher {
  Fetcher fetcher;

  public JavaFetcher(KVStore kvStore, String metaDataSet, Long timeoutMillis, Consumer<LoggableResponse> logFunc, ExternalSourceRegistry registry, String callerName, Boolean disableErrorThrows) {
    this.fetcher = new Fetcher(kvStore, metaDataSet, timeoutMillis, logFunc, false, registry, null, callerName, null, disableErrorThrows, null, TTLCache.DefaultTtlMillis(), TTLCache.DefaultTtlMillis());
  }

  public JavaFetcher(KVStore kvStore, String metaDataSet, Long timeoutMillis, Consumer<LoggableResponse> logFunc, ExternalSourceRegistry registry) {
    this.fetcher = new Fetcher(kvStore, metaDataSet, timeoutMillis, logFunc, false, registry, null, null, null, false, null, TTLCache.DefaultTtlMillis(), TTLCache.DefaultTtlMillis());
  }

  public JavaFetcher(KVStore kvStore, String metaDataSet, Long timeoutMillis, Consumer<LoggableResponse> logFunc, ExternalSourceRegistry registry, ModelPlatformProvider modelPlatformProvider, String callerName, FlagStore flagStore, Boolean disableErrorThrows) {
    this.fetcher = new Fetcher(kvStore, metaDataSet, timeoutMillis, logFunc, false, registry, modelPlatformProvider, callerName, flagStore, disableErrorThrows, null, TTLCache.DefaultTtlMillis(), TTLCache.DefaultTtlMillis());
  }

    /* user builder pattern to create JavaFetcher
    example way to create the java fetcher
    JavaFetcher fetcher = new JavaFetcher.Builder(kvStore, metaDataSet, timeoutMillis, logFunc, registry)
                                        .callerName(callerName)
                                        .flagStore(flagStore)
                                        .disableErrorThrows(disableErrorThrows)
                                        .build();
     */
  private JavaFetcher(Builder builder) {
    this.fetcher = new Fetcher(builder.kvStore,
            builder.metaDataSet,
            builder.timeoutMillis,
            builder.logFunc,
            builder.debug,
            builder.registry,
            builder.modelPlatformProvider,
            builder.callerName,
            builder.flagStore,
            builder.disableErrorThrows,
            builder.executionContextOverride,
            builder.joinConfTtlMillis,
            builder.joinCodecTtlMillis);
  }

  public static class Builder {
    private KVStore kvStore;
    private String metaDataSet;
    private Long timeoutMillis;
    private Consumer<LoggableResponse> logFunc;
    private ExternalSourceRegistry registry;
    private String callerName;
    private boolean debug = false;
    private FlagStore flagStore;
    private boolean disableErrorThrows = false;
    private ExecutionContext executionContextOverride;
    private ModelPlatformProvider modelPlatformProvider;
    private long joinConfTtlMillis = TTLCache.DefaultTtlMillis();
    private long joinCodecTtlMillis = TTLCache.DefaultTtlMillis();

    public Builder(KVStore kvStore, String metaDataSet, Long timeoutMillis,
                   Consumer<LoggableResponse> logFunc, ExternalSourceRegistry registry) {
      this.kvStore = kvStore;
      this.metaDataSet = metaDataSet;
      this.timeoutMillis = timeoutMillis;
      this.logFunc = logFunc;
      this.registry = registry;
    }

    public Builder callerName(String callerName) {
      this.callerName = callerName;
      return this;
    }

    public Builder flagStore(FlagStore flagStore) {
      this.flagStore = flagStore;
      return this;
    }

    public Builder disableErrorThrows(boolean disableErrorThrows) {
      this.disableErrorThrows = disableErrorThrows;
      return this;
    }

    public Builder debug(boolean debug) {
      this.debug = debug;
      return this;
    }

    public Builder executionContextOverride(ExecutionContext executionContextOverride) {
      this.executionContextOverride = executionContextOverride;
      return this;
    }

    public Builder modelPlatformProvider(ModelPlatformProvider modelPlatformProvider) {
      this.modelPlatformProvider = modelPlatformProvider;
      return this;
    }

    public Builder joinConfTtlMillis(long joinConfTtlMillis) {
      this.joinConfTtlMillis = joinConfTtlMillis;
      return this;
    }

    public Builder joinCodecTtlMillis(long joinCodecTtlMillis) {
      this.joinCodecTtlMillis = joinCodecTtlMillis;
      return this;
    }

    public JavaFetcher build() {
      return new JavaFetcher(this);
    }
  }

  private <T extends ai.chronon.online.fetcher.Fetcher.BaseResponse> CompletableFuture<List<JavaResponse>> convertResponsesWithTs(
            Future<FetcherResponseWithTs<T>> responses,
            boolean isGroupBy,
            long startTs) {
    return FutureConverters.toJava(responses).toCompletableFuture().thenApply(resps -> {
        scala.collection.immutable.List<T> scalaList = resps.responses().toList();
        List<JavaResponse> jResps = new ArrayList<>(scalaList.size());
        Iterator<T> it = scalaList.iterator();
        while (it.hasNext()) {
            jResps.add(new JavaResponse(it.next()));
        }
        List<String> requestNames = jResps.stream().map(jResp -> jResp.request.name).collect(Collectors.toList());
        instrument(requestNames, isGroupBy, "java.response_conversion.latency.millis", resps.endTs());
        instrument(requestNames, isGroupBy, "java.overall.latency.millis", startTs);
        return jResps;
    });
  }

  private List<Fetcher.Request> convertJavaRequestList(List<JavaRequest> requests, boolean isGroupBy, long startTs) {
    List<Fetcher.Request> scalaRequests = new ArrayList<>();
    for (JavaRequest request : requests) {
      Fetcher.Request convertedRequest = request.toScalaRequest();
      scalaRequests.add(convertedRequest);
    }
    instrument(requests.stream().map(jReq -> jReq.name).collect(Collectors.toList()), isGroupBy, "java.request_conversion.latency.millis", startTs);
    return scalaRequests;
  }

  public CompletableFuture<List<JavaResponse>> fetchGroupBys(List<JavaRequest> requests) {
    long startTs = System.currentTimeMillis();
    // Convert java requests to scala requests
    List<Fetcher.Request> scalaRequests = convertJavaRequestList(requests, true, startTs);

    // Get responses from the fetcher
    Future<FetcherResponseWithTs<Fetcher.Response>> scalaResponses = this.fetcher.withTs(this.fetcher.fetchGroupBys(ScalaJavaConversions.toScala(scalaRequests)));
    // Convert responses to CompletableFuture
    return convertResponsesWithTs(scalaResponses, true, startTs);
  }

  public CompletableFuture<List<JavaResponse>> fetchJoin(List<JavaRequest> requests) {
    long startTs = System.currentTimeMillis();
    // Convert java requests to scala requests
    List<Fetcher.Request> scalaRequests = convertJavaRequestList(requests, false, startTs);
    // Get responses from the fetcher
    Future<FetcherResponseWithTs<Fetcher.Response>> scalaResponses = this.fetcher.withTs(this.fetcher.fetchJoin(ScalaJavaConversions.toScala(scalaRequests), Option.empty()));
    // Convert responses to CompletableFuture
    return convertResponsesWithTs(scalaResponses, false, startTs);
  }

  public CompletableFuture<List<JavaResponse>> fetchJoinBase64Avro(List<JavaRequest> requests) {
    long startTs = System.currentTimeMillis();
    // Convert java requests to scala requests
    List<Fetcher.Request> scalaRequests = convertJavaRequestList(requests, false, startTs);
    // Get responses from the fetcher
      Future<FetcherResponseWithTs<Fetcher.ResponseV2>> scalaResponses = this.fetcher.withTs(this.fetcher.fetchJoinV2(ScalaJavaConversions.toScala(scalaRequests), Option.empty(), FeaturesResponseType.AvroString()));
    // Convert responses to CompletableFuture
    return convertResponsesWithTs(scalaResponses, false, startTs);
  }

  public CompletableFuture<List<JavaResponse>> fetchModelTransforms(List<JavaRequest> requests) {
    long startTs = System.currentTimeMillis();
    // Convert java requests to scala requests
    List<Fetcher.Request> scalaRequests = convertJavaRequestList(requests, false, startTs);
    // Get responses from the fetcher
    Future<FetcherResponseWithTs<Fetcher.Response>> scalaResponses = this.fetcher.withTs(this.fetcher.fetchModelTransforms(ScalaJavaConversions.toScala(scalaRequests), Option.empty()));
    // Convert responses to CompletableFuture
    return convertResponsesWithTs(scalaResponses, false, startTs);
  }

  public CompletableFuture<List<String>> listJoins(boolean isOnline) {
    // Get responses from the fetcher
    // convert to Java friendly types
    return FutureConverters.toJava(this.fetcher.metadataStore().listJoins(isOnline)).toCompletableFuture().thenApply(ScalaJavaConversions::toJava);
  }

  public JTry<JavaJoinSchemaResponse> fetchJoinSchema(String joinName) {
    Try<Fetcher.JoinSchemaResponse> scalaResponse = this.fetcher.fetchJoinSchema(joinName);
    return JTry.fromScala(scalaResponse).map(JavaJoinSchemaResponse::new);
  }

  public JTry<JavaGroupBySchemaResponse> fetchGroupBySchema(String groupByName) {
    Try<Fetcher.GroupBySchemaResponse> scalaResponse = this.fetcher.fetchGroupBySchema(groupByName);
    return JTry.fromScala(scalaResponse).map(JavaGroupBySchemaResponse::new);
  }

  public JTry<JavaGroupByStatusResponse> fetchGroupByStatus(String groupByName) {
    Try<Fetcher.GroupByStatusResponse> scalaResponse = this.fetcher.fetchGroupByStatus(groupByName);
    return JTry.fromScala(scalaResponse).map(JavaGroupByStatusResponse::new);
  }

  private void instrument(List<String> requestNames, boolean isGroupBy, String metricName, Long startTs) {
    long endTs = System.currentTimeMillis();
    for (String s : requestNames) {
      Metrics.Context ctx;
      if (isGroupBy) {
        ctx = getGroupByContext(s);
      } else {
        ctx = getJoinContext(s);
      }
      ctx.distribution(metricName, endTs - startTs);
    }
  }

  private Metrics.Context getJoinContext(String joinName) {
    return new Metrics.Context("join.fetch", joinName, null, null, false, null, null, null, null, null, null, null);
  }

  private Metrics.Context getGroupByContext(String groupByName) {
    return new Metrics.Context("group_by.fetch", null, groupByName, null, false, null, null, null, null, null, null, null);
  }
}
