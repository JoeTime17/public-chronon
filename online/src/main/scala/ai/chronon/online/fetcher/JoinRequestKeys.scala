package ai.chronon.online.fetcher

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online._
import ai.chronon.online.fetcher.Fetcher.Request
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

import scala.collection.mutable
import scala.util.Try

/** Translates a Join request's keys into the keys needed by each underlying GroupBy fetch.
  *
  * GroupBys are fetched by their source keys, but a Join can define left-side selects that derive those keys from the
  * raw request. For example, a Join may receive `query`, select `query_normalized = lower(query)`, and then map
  * `query_normalized` to a GroupBy key. In that case the online Join fetch path should accept `query` from callers,
  * derive `query_normalized` inside the fetcher, and use the derived value only when building the GroupBy request.
  *
  * The helper keeps that flow local to Join fetching:
  *   - `buildKeyMapping` is run while building the JoinCodec so SQL parsing, input-schema construction, and Catalyst
  *     wrapper setup are cached with the Join metadata instead of repeated per request.
  *   - `requestKeyFields` reports the raw request keys needed to derive selected Join keys, plus the derived key aliases
  *     themselves so existing direct-key callers can still be logged against the Join key schema.
  *   - `valueInfoLeftKeys` reports only the raw request keys for selected Join keys, so fetchJoinSchema does not imply
  *     that callers must provide derived keys.
  *   - `missingRequestKeys` validates against those raw inputs, while still accepting a derived key if the caller
  *     provides it directly.
  *   - `KeyMapping.leftKeys` runs the cached Join left-select transform when needed, then combines derived and directly
  *     supplied left keys before JoinPartFetcher maps them to right-side GroupBy keys.
  */
private[online] object JoinRequestKeys {
  case class KeyMapping(leftToRight: Map[String, String],
                        requestKeyFields: Iterable[StructField],
                        rawInputsByLeftKey: Map[String, Seq[String]],
                        selectedLeftKeys: Seq[(String, String)],
                        catalystUtil: Option[PooledCatalystUtil]) {
    private val selectedExpressions = selectedLeftKeys.toMap

    private def shouldDeriveLeftKey(request: Request, leftKey: String): Boolean =
      selectedExpressions.contains(leftKey) &&
        (!request.keys.contains(leftKey) || rawInputsByLeftKey.getOrElse(leftKey, Seq.empty).contains(leftKey))

    def missingRequestKeys(request: Request): Seq[String] =
      leftToRight.keys.toSeq.flatMap { leftKey =>
        if (request.keys.contains(leftKey)) {
          Seq.empty
        } else {
          rawInputsByLeftKey
            .get(leftKey)
            .filter(_ => selectedExpressions.contains(leftKey))
            .getOrElse(Seq(leftKey))
            .filterNot(request.keys.contains)
        }
      }.distinct

    def leftKeys(request: Request): Map[String, AnyRef] = {
      val directLeftKeys = leftToRight.keys.collect {
        case leftKey if request.keys.contains(leftKey) && !shouldDeriveLeftKey(request, leftKey) =>
          leftKey -> request.keys(leftKey)
      }.toMap

      val keysToDerive = selectedLeftKeys.filter { case (leftKey, _) => shouldDeriveLeftKey(request, leftKey) }
      val derivedLeftKeys =
        if (keysToDerive.isEmpty) {
          Map.empty[String, Any]
        } else {
          val derivedValues = catalystUtil
            .map(_.performSql(request.keys).headOption.getOrElse(Map.empty))
            .getOrElse(Map.empty)
          keysToDerive.map { case (leftKey, _) =>
            leftKey -> derivedValues.getOrElse(leftKey, null)
          }.toMap
        }

      (directLeftKeys ++ derivedLeftKeys).map { case (key, value) => key -> value.asInstanceOf[AnyRef] }
    }
  }

  private def partKey(parts: Seq[String]): String =
    parts.map(part => s"${part.length}:$part").mkString("|")

  private def partKeyParts(join: Join, joinPart: JoinPartOps): Seq[String] =
    Seq(
      joinPart.groupBy.metaData.getName,
      Option(joinPart.prefix).getOrElse("")
    ) ++
      joinPart.leftToRight.toSeq.sortBy(_._1).map { case (left, right) => s"key:$left=$right" } ++
      leftSelects(join).toSeq.sortBy(_._1).map { case (name, expr) => s"select:$name=$expr" } ++
      leftSetups(join).map(setup => s"setup:$setup")

  def partKey(join: Join, joinPart: JoinPartOps): String =
    partKey(partKeyParts(join, joinPart))

  def partKey(join: Join, joinPart: JoinPartOps, servingInfo: GroupByServingInfoParsed): String =
    partKey(partKeyParts(join, joinPart) ++ Seq(servingInfo.keyAvroSchema, servingInfo.inputAvroSchema))

  private def leftSelects(join: Join): Map[String, String] =
    Option(join.left)
      .flatMap(source => Option(source.query))
      .flatMap(query => Option(query.getQuerySelects))
      .getOrElse(Map.empty)

  private def leftSetups(join: Join): Seq[String] =
    Option(join.left)
      .flatMap(source => Option(source.query))
      .map(_.setupsSeq)
      .getOrElse(Seq.empty)

  private def selectExpression(join: Join, leftKey: String): Option[String] =
    leftSelects(join).get(leftKey).filter(_ != leftKey)

  private[fetcher] def rawInputs(expression: String): Seq[String] =
    CatalystSqlParser
      .parseExpression(expression)
      .collect { case attr: UnresolvedAttribute =>
        attr.nameParts.head
      }
      .distinct

  private def rawInputType(servingInfo: GroupByServingInfoParsed,
                           requestKey: String,
                           fallbackType: DataType): DataType =
    Try(servingInfo.inputChrononSchema.typeOf(requestKey)).toOption.flatten.getOrElse(fallbackType)

  def valueInfoLeftKeys(join: Join, joinPart: JoinPartOps): Iterable[String] =
    joinPart.leftToRight.keys.toSeq.flatMap { leftKey =>
      selectExpression(join, leftKey).map(rawInputs).getOrElse(Seq(leftKey))
    }.distinct

  private def requestKeyFields(join: Join,
                               joinPart: JoinPartOps,
                               servingInfo: GroupByServingInfoParsed,
                               rawInputsByLeftKey: Map[String, Seq[String]]): Iterable[StructField] = {
    val keySchema = servingInfo.keyCodec.chrononSchema.asInstanceOf[StructType]
    val fieldsByRightKey = keySchema.fields.map(field => field.name -> field).toMap
    val fieldsByRequestKey = mutable.LinkedHashMap.empty[String, StructField]

    joinPart.leftToRight.foreach { case (leftKey, rightKey) =>
      val fieldType = fieldsByRightKey
        .getOrElse(
          rightKey,
          throw new IllegalArgumentException(
            s"Join part ${joinPart.fullPrefix} maps left key $leftKey to right key $rightKey, " +
              s"but $rightKey is not present in GroupBy key schema ${keySchema.fields.map(_.name).mkString(", ")}")
        )
        .fieldType
      val requestKeys = rawInputsByLeftKey.getOrElse(leftKey, Seq(leftKey))
      requestKeys.foreach { requestKey =>
        if (!fieldsByRequestKey.contains(requestKey)) {
          fieldsByRequestKey.put(requestKey, StructField(requestKey, rawInputType(servingInfo, requestKey, fieldType)))
        }
      }
      if (!fieldsByRequestKey.contains(leftKey)) {
        fieldsByRequestKey.put(leftKey, StructField(leftKey, fieldType))
      }
    }

    fieldsByRequestKey.values
  }

  def buildKeyMapping(join: Join, joinPart: JoinPartOps, servingInfo: GroupByServingInfoParsed): KeyMapping = {
    val selectedLeftKeys = joinPart.leftToRight.keys.toSeq.flatMap { leftKey =>
      selectExpression(join, leftKey).map(leftKey -> _)
    }
    val rawInputsByLeftKey = joinPart.leftToRight.keys.map { leftKey =>
      leftKey -> selectedLeftKeys
        .find(_._1 == leftKey)
        .map { case (_, expression) =>
          rawInputs(expression)
        }
        .getOrElse(Seq(leftKey))
    }.toMap
    val keyFields = requestKeyFields(join, joinPart, servingInfo, rawInputsByLeftKey)
    val catalystUtil =
      if (selectedLeftKeys.isEmpty) None
      else
        Some(new PooledCatalystUtil(selectedLeftKeys, StructType("JoinRequest", keyFields.toArray), leftSetups(join)))

    KeyMapping(joinPart.leftToRight, keyFields, rawInputsByLeftKey, selectedLeftKeys, catalystUtil)
  }

  def missingRequestKeys(request: Request, join: Join, joinPart: JoinPartOps): Seq[String] =
    joinPart.leftToRight.keys.toSeq.flatMap { leftKey =>
      if (request.keys.contains(leftKey)) {
        Seq.empty
      } else {
        selectExpression(join, leftKey).map(rawInputs).getOrElse(Seq(leftKey)).filterNot(request.keys.contains)
      }
    }.distinct

  private def shouldDeriveLeftKey(request: Request, join: Join, leftKey: String): Boolean =
    selectExpression(join, leftKey).exists { expression =>
      !request.keys.contains(leftKey) || rawInputs(expression).contains(leftKey)
    }

  def needsDerivation(request: Request, join: Join, joinPart: JoinPartOps): Boolean =
    joinPart.leftToRight.keys.exists(shouldDeriveLeftKey(request, join, _))

  def deriveLeftKeys(request: Request,
                     join: Join,
                     joinPart: JoinPartOps,
                     servingInfo: GroupByServingInfoParsed): Map[String, AnyRef] = {
    buildKeyMapping(join, joinPart, servingInfo).leftKeys(request)
  }
}
