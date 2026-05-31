package ai.chronon.spark

// Re-export submission types from canonical location in api module.
// Existing code importing from ai.chronon.spark.submission continues to work.
package object submission {
  type JobType = ai.chronon.api.submission.JobType
  val SparkJob = ai.chronon.api.submission.SparkJob
  val FlinkJob = ai.chronon.api.submission.FlinkJob

  type JobSubmitter = ai.chronon.api.submission.JobSubmitter
  val JobSubmitter = ai.chronon.api.submission.JobSubmitter

  val JobSubmitterConstants = ai.chronon.api.submission.JobSubmitterConstants

  type StorageClient = ai.chronon.api.submission.StorageClient
  val StorageClient = ai.chronon.api.submission.StorageClient
}
