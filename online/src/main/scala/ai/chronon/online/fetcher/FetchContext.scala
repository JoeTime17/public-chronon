package ai.chronon.online.fetcher
import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.ScalaJavaConversions.JMapOps
import ai.chronon.online.metrics.FlexibleExecutionContext
import ai.chronon.online.metrics.TTLCache
import ai.chronon.online.{FlagStore, FlagStoreConstants, KVStore}

import scala.concurrent.ExecutionContext

case class FetchContext(kvStore: KVStore,
                        metadataDataset: String = MetadataDataset,
                        timeoutMillis: Long = 10000,
                        debug: Boolean = false,
                        flagStore: FlagStore = null,
                        disableErrorThrows: Boolean = false,
                        executionContextOverride: ExecutionContext = null,
                        joinConfTtlMillis: Long = TTLCache.DefaultTtlMillis,
                        joinCodecTtlMillis: Long = TTLCache.DefaultTtlMillis,
                        kvTimeoutMillis: Long = FetchContext.DefaultKvTimeoutMillis) {

  def getOrCreateExecutionContext: ExecutionContext = {
    Option(executionContextOverride).getOrElse(FlexibleExecutionContext.buildExecutionContext)
  }

  // TODO: delete this flagStore plz
  def isTilingEnabled: Boolean = {
    Option(flagStore)
      .map(_.isSet(FlagStoreConstants.TILING_ENABLED, Map.empty[String, String].toJava))
      .exists(_.asInstanceOf[Boolean])
  }
}

object FetchContext {
  val KvTimeoutMillisProperty = "ai.chronon.fetcher.kv.timeout.millis"
  val DefaultKvTimeoutMillis: Long =
    Option(System.getProperty(KvTimeoutMillisProperty))
      .flatMap(raw => scala.util.Try(raw.trim.toLong).toOption)
      .getOrElse(10000L)
}
