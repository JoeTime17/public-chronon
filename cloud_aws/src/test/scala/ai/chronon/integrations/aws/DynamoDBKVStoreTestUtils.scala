package ai.chronon.integrations.aws

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.collection.mutable

/** Subclass of DynamoDBKVStoreImpl that records calls to addReplicaRegions instead of executing them.
  *
  * DynamoDB Local does not support Global Tables operations, so we can't verify replica update
  * behavior end-to-end. This spy captures (tableName, regions) pairs so tests can assert that
  * addReplicaRegions was called with the correct arguments — and that it was not called when config
  * is absent.
  */
class SpyingDynamoDBKVStore(client: DynamoDbAsyncClient, conf: Map[String, String] = Map.empty)
    extends DynamoDBKVStoreImpl(client, conf) {

  // Each element is (tableName, resolvedRegions) captured from an addReplicaRegions call
  val replicaCalls: mutable.ListBuffer[(String, List[String])] = mutable.ListBuffer.empty

  override protected def addReplicaRegions(tableName: String): Unit = {
    import ai.chronon.api.Constants.KvReplicaRegionsArg
    val regions = conf
      .get(KvReplicaRegionsArg)
      .filter(_.nonEmpty)
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toList)
      .getOrElse(List.empty)
    // Mirror the real impl's early-exit: only record if there are actual regions to replicate to
    if (regions.nonEmpty) replicaCalls += ((tableName, regions))
  }
}
