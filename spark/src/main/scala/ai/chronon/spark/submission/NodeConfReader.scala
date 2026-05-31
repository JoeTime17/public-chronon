package ai.chronon.spark.submission

import ai.chronon.api.ThriftJsonCodec
import ai.chronon.planner.Node

import java.util.Locale
import scala.io.Source

/** Reads node configs through Hadoop FS so Crucible-submitted jobs can load staged configs from object storage. */
object NodeConfReader {

  def read(confPath: String): Node = {
    if (isCloudUri(confPath)) {
      readFromCloudStorage(confPath)
    } else {
      ThriftJsonCodec.fromJsonFile[Node](confPath, check = false)
    }
  }

  private[submission] def isCloudUri(path: String): Boolean = {
    val lowerPath = path.toLowerCase(Locale.ROOT)
    lowerPath.startsWith("gs://") ||
    lowerPath.startsWith("s3://") ||
    lowerPath.startsWith("s3a://") ||
    lowerPath.startsWith("abfs://") ||
    lowerPath.startsWith("abfss://")
  }

  private def readFromCloudStorage(confPath: String): Node = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    hadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoopConf.set("fs.gs.auth.type", "APPLICATION_DEFAULT")
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    // S3A in Hadoop 3.4+ defaults to the v2 SDK chain that does NOT include
    // WebIdentity, so on EKS IRSA the conf read fails with
    // NoAuthWithAWSException. Force the v2 WebIdentity provider explicitly —
    // the EKS-injected AWS_WEB_IDENTITY_TOKEN_FILE / AWS_ROLE_ARN env vars are
    // all this provider needs to resolve credentials.
    hadoopConf.set("fs.s3a.aws.credentials.provider",
                   "software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider")
    hadoopConf.set("fs.abfs.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem")
    hadoopConf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem")
    hadoopConf.set("fs.azure.account.auth.type", "OAuth")
    hadoopConf.set("fs.azure.account.oauth.provider.type",
                   "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider")
    sys.env.get("AZURE_CLIENT_ID").foreach(hadoopConf.set("fs.azure.account.oauth2.client.id", _))
    sys.env.get("AZURE_TENANT_ID").foreach(hadoopConf.set("fs.azure.account.oauth2.msi.tenant", _))
    sys.env.get("AZURE_FEDERATED_TOKEN_FILE").foreach(hadoopConf.set("fs.azure.account.oauth2.token.file", _))

    val path = new org.apache.hadoop.fs.Path(confPath)
    val fs = org.apache.hadoop.fs.FileSystem.get(path.toUri, hadoopConf)
    val stream = fs.open(path)
    val source = Source.fromInputStream(stream, "UTF-8")
    try {
      val json = source.mkString
      ThriftJsonCodec.fromJson[Node](json, check = false)
    } finally {
      source.close()
    }
  }
}
