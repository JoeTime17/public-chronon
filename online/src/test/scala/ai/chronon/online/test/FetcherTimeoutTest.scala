package ai.chronon.online.test

import ai.chronon.online.fetcher.FetcherTimeout
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.TimeoutException
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class FetcherTimeoutTest extends AnyFlatSpec {

  implicit val ec: ExecutionContext = ExecutionContext.global

  it should "return the result when the future completes before timeout" in {
    val future = Future.successful("hello")
    val result = Await.result(FetcherTimeout.withTimeout(future, 5000), 1.second)
    assert(result == "hello")
  }

  it should "fail with TimeoutException when the future exceeds timeout" in {
    val promise = Promise[String]()
    val timedFuture = FetcherTimeout.withTimeout(promise.future, 100)
    val result = Await.ready(timedFuture, 2.seconds).value.get
    assert(result.isFailure)
    assert(result.failed.get.isInstanceOf[TimeoutException])
    assert(result.failed.get.getMessage.contains("100ms"))
  }

  it should "pass through the original future when timeout is 0" in {
    val future = Future.successful(42)
    val result = FetcherTimeout.withTimeout(future, 0)
    assert(result eq future)
  }

  it should "propagate the original exception when the future fails before timeout" in {
    val future = Future.failed[String](new RuntimeException("test error"))
    val timedFuture = FetcherTimeout.withTimeout(future, 5000)
    val result = Await.ready(timedFuture, 1.second).value.get
    assert(result.isFailure)
    assert(result.failed.get.isInstanceOf[RuntimeException])
    assert(result.failed.get.getMessage == "test error")
  }
}
