package ai.chronon.online.test

import ai.chronon.online.metrics.FlexibleExecutionContext
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}

class FlexibleExecutionContextTest extends AnyFlatSpec {

  it should "use default pool size of cores * 4 when no system property is set" in {
    val executor = FlexibleExecutionContext.buildExecutor
    val expectedPoolSize = Runtime.getRuntime.availableProcessors() * 4
    assert(executor.getCorePoolSize == expectedPoolSize)
    assert(executor.getMaximumPoolSize == expectedPoolSize)
    assert(executor.getKeepAliveTime(TimeUnit.SECONDS) == 600)
  }

  it should "have a bounded blocking queue" in {
    val executor = FlexibleExecutionContext.buildExecutor
    assert(executor.getQueue.isInstanceOf[ArrayBlockingQueue[_]])
    val queue = executor.getQueue.asInstanceOf[ArrayBlockingQueue[_]]
    assert(queue.remainingCapacity() == 10000)
  }

  it should "expose configurable property key constants" in {
    assert(FlexibleExecutionContext.ThreadPoolSizeProperty == "ai.chronon.threadpool.size")
    assert(FlexibleExecutionContext.QueueCapacityProperty == "ai.chronon.threadpool.queue.capacity")
    assert(FlexibleExecutionContext.KeepAliveSecondsProperty == "ai.chronon.threadpool.keepalive.seconds")
  }
}
