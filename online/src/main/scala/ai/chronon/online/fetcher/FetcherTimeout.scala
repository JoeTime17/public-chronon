package ai.chronon.online.fetcher

import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit, TimeoutException}
import scala.concurrent.{ExecutionContext, Future, Promise}

object FetcherTimeout {
  private val scheduler = {
    val s = new ScheduledThreadPoolExecutor(1,
                                            (r: Runnable) => {
                                              val t = new Thread(r, "chronon-fetcher-timeout")
                                              t.setDaemon(true)
                                              t
                                            })
    s.setRemoveOnCancelPolicy(true)
    s
  }

  def withTimeout[T](future: Future[T], millis: Long)(implicit ec: ExecutionContext): Future[T] = {
    if (millis <= 0) return future

    val promise = Promise[T]()
    val timeout = scheduler.schedule(
      new Runnable {
        override def run(): Unit =
          promise.tryFailure(new TimeoutException(s"KV store multiGet timed out after ${millis}ms"))
      },
      millis,
      TimeUnit.MILLISECONDS
    )
    future.onComplete { result =>
      timeout.cancel(false)
      promise.tryComplete(result)
    }
    promise.future
  }
}
