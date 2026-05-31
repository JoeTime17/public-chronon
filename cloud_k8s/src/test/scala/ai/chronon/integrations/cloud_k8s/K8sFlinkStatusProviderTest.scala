package ai.chronon.integrations.cloud_k8s

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class K8sFlinkStatusProviderTest extends AnyFlatSpec with Matchers {

  private val provider = new K8sFlinkStatusProvider()

  "parseJobId" should "extract job ID from a valid jobs response" in {
    val jobsJson =
      """{
        "jobs": [
          {
            "id": "f7b21fcde2af4e99468c944b3280855c",
            "status": "RUNNING"
          }
        ]
      }"""
    provider.parseJobId(jobsJson) shouldBe Some("f7b21fcde2af4e99468c944b3280855c")
  }

  it should "return None for an empty jobs array" in {
    provider.parseJobId("""{"jobs": []}""") shouldBe None
  }

  it should "return None when the jobs field is absent" in {
    provider.parseJobId("""{"other": "field"}""") shouldBe None
  }

  it should "return None for invalid JSON" in {
    provider.parseJobId("not-json") shouldBe None
  }

  it should "return the first job ID when multiple jobs are present" in {
    val jobsJson =
      """{
        "jobs": [
          {"id": "first-job-id", "status": "RUNNING"},
          {"id": "second-job-id", "status": "RUNNING"}
        ]
      }"""
    provider.parseJobId(jobsJson) shouldBe Some("first-job-id")
  }

  "parseCheckpointCounts" should "extract the completed count from a valid checkpoint response" in {
    val checkpointJson =
      """{
        "counts": {
          "restored": 1,
          "total": 56162,
          "in_progress": 0,
          "completed": 56161,
          "failed": 1
        }
      }"""
    provider.parseCheckpointCounts(checkpointJson) shouldBe Some(56161)
  }

  it should "return None when the counts object is missing" in {
    provider.parseCheckpointCounts("""{"summary": {}}""") shouldBe None
  }

  it should "return 0 when the completed field is absent" in {
    provider.parseCheckpointCounts("""{"counts": {"total": 100}}""") shouldBe Some(0)
  }

  it should "return None for invalid JSON" in {
    provider.parseCheckpointCounts("not-json") shouldBe None
  }

  "isHealthyBasedOnCheckpoints" should "return true when completed checkpoints >= 3" in {
    provider.isHealthyBasedOnCheckpoints(3) shouldBe true
    provider.isHealthyBasedOnCheckpoints(4) shouldBe true
    provider.isHealthyBasedOnCheckpoints(100) shouldBe true
  }

  it should "return false when completed checkpoints < 3" in {
    provider.isHealthyBasedOnCheckpoints(0) shouldBe false
    provider.isHealthyBasedOnCheckpoints(1) shouldBe false
    provider.isHealthyBasedOnCheckpoints(2) shouldBe false
  }

  it should "handle the boundary case of exactly 3 completed checkpoints" in {
    val json =
      """{
        "counts": {
          "restored": 0,
          "total": 3,
          "in_progress": 0,
          "completed": 3,
          "failed": 0
        }
      }"""
    val count = provider.parseCheckpointCounts(json)
    count shouldBe Some(3)
    provider.isHealthyBasedOnCheckpoints(count.get) shouldBe true
  }

  it should "handle the boundary case of exactly 2 completed checkpoints (unhealthy)" in {
    val json =
      """{
        "counts": {
          "restored": 0,
          "total": 2,
          "in_progress": 0,
          "completed": 2,
          "failed": 0
        }
      }"""
    val count = provider.parseCheckpointCounts(json)
    count shouldBe Some(2)
    provider.isHealthyBasedOnCheckpoints(count.get) shouldBe false
  }
}
