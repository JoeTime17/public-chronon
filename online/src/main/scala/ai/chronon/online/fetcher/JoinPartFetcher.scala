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

package ai.chronon.online.fetcher

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online._
import ai.chronon.online.fetcher.Fetcher.{ColumnSpec, PrefixedRequest, Request, Response}
import ai.chronon.online.fetcher.FetcherCache.BatchResponses
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class JoinPartFetcher(fetchContext: FetchContext, metadataStore: MetadataStore) {

  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private[online] val groupByFetcher = new GroupByFetcher(fetchContext, metadataStore)
  private implicit val executionContext: ExecutionContext = fetchContext.getOrCreateExecutionContext

  def fetchGroupBys(requests: Seq[Request]): Future[Seq[Response]] = {
    groupByFetcher.fetchGroupBys(requests)
  }

  // ----- START -----
  // floated up to makes tests easy
  def fetchColumns(specs: Seq[ColumnSpec]): Future[Map[ColumnSpec, Response]] = {
    groupByFetcher.fetchColumns(specs)
  }

  def getServingInfo(existing: GroupByServingInfoParsed, batchResponses: BatchResponses): GroupByServingInfoParsed = {
    groupByFetcher.getServingInfo(existing, batchResponses)
  }

  def isCacheSizeConfigured: Boolean = {
    groupByFetcher.isCacheSizeConfigured
  }
  // ---- END ----

  // prioritize passed in joinOverrides over the ones in metadata store
  // used in stream-enrichment and in staging testing
  def fetchJoins(requests: Seq[Request],
                 joinConf: Option[Join] = None,
                 joinCodecForName: String => Option[Try[JoinCodec]] = _ => None): Future[Seq[Response]] = {
    val startTimeMs = System.currentTimeMillis()
    // convert join requests to groupBy requests
    val joinDecomposed: Seq[(Request, Try[Seq[Either[PrefixedRequest, KeyMissingException]]])] =
      requests.map { request =>
        // use passed-in join or fetch one
        val joinTry: Try[JoinOps] = if (joinConf.isEmpty) {
          val joinConfTry = metadataStore.getJoinConf(request.name)
          if (joinConfTry.isFailure) {
            metadataStore.getJoinConf.refresh(request.name)
          }
          joinConfTry
        } else {
          logger.debug(s"Using passed in join configuration: ${joinConf.get.metaData.getName}")
          Success(JoinOps(joinConf.get))
        }

        var joinContext: Option[metrics.Metrics.Context] = None

        val decomposedTry = joinTry.map { join =>
          import ai.chronon.online.metrics
          joinContext = Some(metrics.Metrics.Context(metrics.Metrics.Environment.JoinFetching, join.join))
          joinContext.get.increment("join_request.count")

          join.joinPartOps.map { part =>
            import ai.chronon.online.metrics
            val joinContextInner = metrics.Metrics.Context(joinContext.get, part)
            val needsDerivation = JoinRequestKeys.needsDerivation(request, join.join, part)
            lazy val currentServingInfo = metadataStore.getGroupByServingInfo(part.groupBy.metaData.getName)
            lazy val keyMapping = joinCodecForName(request.name).flatMap(_.toOption).flatMap { codec =>
              currentServingInfo.toOption
                .flatMap(servingInfo =>
                  codec.joinPartKeyMappings.get(JoinRequestKeys.partKey(join.join, part, servingInfo)))
                .orElse {
                  if (currentServingInfo.isFailure) {
                    codec.joinPartKeyMappings.get(JoinRequestKeys.partKey(join.join, part))
                  } else {
                    None
                  }
                }
            }
            val staticMissingKeys = JoinRequestKeys.missingRequestKeys(request, join.join, part)
            val missingKeys =
              if (staticMissingKeys.nonEmpty || !needsDerivation) {
                staticMissingKeys
              } else {
                keyMapping.map(_.missingRequestKeys(request)).getOrElse(staticMissingKeys)
              }

            if (missingKeys.nonEmpty) {
              Right(KeyMissingException(part.fullPrefix, missingKeys.toSeq, request.keys))
            } else {
              val leftKeys =
                if (needsDerivation) {
                  keyMapping.map(_.leftKeys(request)).getOrElse {
                    val servingInfo = currentServingInfo.get
                    JoinRequestKeys.deriveLeftKeys(request, join.join, part, servingInfo)
                  }
                } else {
                  part.leftToRight.keys.map(leftKey => leftKey -> request.keys(leftKey)).toMap
                }
              val rightKeys = part.leftToRight.map { case (leftKey, rightKey) => rightKey -> leftKeys(leftKey) }
              Left(
                PrefixedRequest(
                  part.columnPrefix,
                  Request(part.groupBy.getMetaData.getName, rightKeys, request.atMillis, Some(joinContextInner))))
            }

          }
        }
        request.copy(context = joinContext) -> decomposedTry
      }

    val groupByRequests = joinDecomposed.flatMap { case (_, gbTry) =>
      gbTry match {
        case Failure(_)        => Iterator.empty
        case Success(requests) => requests.iterator.flatMap(_.left.toOption).map(_.request)
      }
    }

    val groupByResponsesFuture = fetchGroupBys(groupByRequests)

    // re-attach groupBy responses to join
    groupByResponsesFuture
      .map { groupByResponses =>
        val responseMap = groupByResponses.iterator.map { response => response.request -> response.values }.toMap
        val responses = joinDecomposed.iterator.map { case (joinRequest, decomposedRequestsTry) =>
          val joinValuesTry = decomposedRequestsTry.map { groupByRequestsWithPrefix =>
            groupByRequestsWithPrefix.iterator.flatMap {

              case Right(keyMissingException) =>
                Map(
                  keyMissingException.requestName + FetcherUtil.FeatureExceptionSuffix -> keyMissingException.getMessage)

              case Left(PrefixedRequest(prefix, groupByRequest)) =>
                parseGroupByResponse(prefix, groupByRequest, responseMap)
            }.toMap

          }
          joinValuesTry match {
            case Failure(ex) => joinRequest.context.foreach(_.incrementException(ex))
            case Success(responseMap) =>
              joinRequest.context.foreach { ctx =>
                ctx.distribution("response.keys.count", responseMap.size)
                Fetcher.logFeatureNullRates(responseMap, ctx)
              }
          }
          joinRequest.context.foreach { ctx =>
            ctx.distribution("internal.latency.millis", System.currentTimeMillis() - startTimeMs)
            ctx.increment("internal.request.count")
          }
          Response(joinRequest, joinValuesTry)
        }.toSeq
        responses
      }
  }

  def parseGroupByResponse(prefix: String,
                           groupByRequest: Request,
                           responseMap: Map[Request, Try[Map[String, AnyRef]]]): Map[String, AnyRef] = {
    // Group bys with all null keys won't be requested from the KV store and we don't expect a response.
    val isRequiredRequest = groupByRequest.keys.values.exists(_ != null) || groupByRequest.keys.isEmpty

    val response: Try[Map[String, AnyRef]] = responseMap.get(groupByRequest) match {
      case Some(value) => value
      case None =>
        if (isRequiredRequest)
          Failure(new IllegalStateException(s"Couldn't find a groupBy response for $groupByRequest in response map"))
        else Success(null)
    }

    response
      .map { valueMap =>
        if (valueMap != null) {
          valueMap.map { case (aggName, aggValue) => prefix + aggName -> aggValue }
        } else {
          Map.empty[String, AnyRef]
        }
      }
      // prefix feature names
      .recover { // capture exception as a key
        case ex: Throwable =>
          if (fetchContext.debug || Math.random() < 0.001) {
            println(s"Failed to fetch $groupByRequest with \n${ex.traceString}")
          }
          Map(prefix + "exception" -> ex.traceString)
      }
      .get
  }
}
