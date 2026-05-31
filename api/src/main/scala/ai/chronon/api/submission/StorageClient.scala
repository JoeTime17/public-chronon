package ai.chronon.api.submission

trait StorageClient {

  def downloadObjectToMemory(objectPath: String): Array[Byte]

  def listFiles(bucketPath: String): Iterator[String]

  def fileExists(objectPath: String): Boolean

  def upload(objectPath: String, content: Array[Byte]): Unit

}

object StorageClient {

  /** Lists `{flinkStateUri}/checkpoints/{flinkInternalJobId}/chk-*` via the given client and
    * returns the path to the highest-numbered checkpoint, or None if none exist.
    */
  def resolveLatestCheckpointPath(client: StorageClient,
                                  flinkInternalJobId: String,
                                  flinkStateUri: String): Option[String] = {
    val jobCheckpointPath = s"$flinkStateUri/checkpoints/$flinkInternalJobId"
    val latestCheckpoint = client
      .listFiles(jobCheckpointPath)
      .filter(_.split("/").exists(_.startsWith("chk-")))
      .map(_.split("/").find(_.startsWith("chk-")).get)
      .toList
      .distinct
      .sortBy(_.substring(4).toInt)(Ordering.Int.reverse)
      .headOption
    latestCheckpoint.map(chk => s"$jobCheckpointPath/$chk")
  }
}
