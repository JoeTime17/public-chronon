package ai.chronon.api.planner

import ai.chronon.api
import ai.chronon.api.PartitionRange
import ai.chronon.planner.NodeContent
trait NodeRunner {

  def run(metadata: api.MetaData, conf: NodeContent, range: Option[PartitionRange]): Unit
}

object NodeRunner {}

object LineageOfflineRunner {
  def readFiles(folderPath: String): Seq[Any] = {
    // read files from folder using metadata
    Seq.empty
  }
}
