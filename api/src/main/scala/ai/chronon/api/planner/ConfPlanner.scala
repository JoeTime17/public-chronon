package ai.chronon.api.planner

import ai.chronon.api.thrift.TBase
import ai.chronon.api.Extensions._
import ai.chronon.api.{MetaData, PartitionSpec, ThriftJsonCodec}
import ai.chronon.planner.{ConfPlan, Node, NodeContent}

case class Mode(name: String, nodes: Seq[Node], cron: String)

/** Base class for Chronon orchestration node planning. Corresponds 1:1 with a Chronon
  * conf type (e.g. Join, GroupBy, StagingQuery, ...).
  * The concrete planner implementations are responsible for constructing all the Nodes of the conf (for various
  * supported modes, e.g. BACKFILL, DEPLOY) and returning a ConfPlan.
  */
abstract class ConfPlanner[T](conf: T)(implicit outputPartitionSpec: PartitionSpec) {

  def buildPlan: ConfPlan

  def toNode[T <: TBase[_, _]: Manifest](metaData: MetaData,
                                         contentSetter: NodeContent => Unit,
                                         hashableNode: T): Node = {
    val baseHash = ThriftJsonCodec.hexDigest(hashableNode)
    val hash = Option(metaData).map(_.mixGridToken(baseHash)).getOrElse(baseHash)

    val content = new NodeContent()
    contentSetter(content)

    val node =
      new Node()
        .setContent(content)
        .setMetaData(metaData)
        .setSemanticHash(hash)

    content.getSetField match {
      case NodeContent._Fields.GROUP_BY_STREAMING => node.setIsLongRunning(true)
      case _                                      => node.setIsLongRunning(false)
    }
    node
  }
}
