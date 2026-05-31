package ai.chronon.integrations.cloud_gcp

import org.slf4j.LoggerFactory

import java.io.{BufferedReader, InputStreamReader}
import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/** Launcher for Flink Application Mode on Dataproc.
  *
  * Runs as a HadoopJob on the Dataproc cluster master. Constructs and executes
  * `flink run-application -t yarn-application` via ProcessBuilder, then blocks
  * while monitoring the YARN application status. This keeps the Dataproc HadoopJob
  * in RUNNING state, preserving compatibility with existing monitoring infrastructure.
  *
  * Flink 1.17's YarnApplicationFileUploader does not support gs:// URIs for the main
  * JAR or pipeline JARs. This launcher stages GCS JARs to HDFS before submission.
  */
object FlinkApplicationLauncher {
  private val logger = LoggerFactory.getLogger(getClass)

  val DefaultFlinkBinPath = "/usr/lib/flink/bin/flink"
  val YarnAppIdPattern: Regex = """application_\d+_\d+""".r
  val YarnStatusStatePattern: Regex = """(?i)\s*State\s*:\s*(\S+)""".r
  val PollIntervalMs: Long = 30000
  val HdfsStagingBase = "/tmp/flink-app-mode"
  val LocalStagingBase = "/tmp/flink-app-mode-local"

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    val jobUuid = UUID.randomUUID().toString
    val hdfsStagingDir = s"$HdfsStagingBase/$jobUuid"
    val localStagingDir = s"$LocalStagingBase/$jobUuid"
    val stagedConfig = stageGcsJars(config, localStagingDir, hdfsStagingDir)

    // Ensure Flink distribution JARs are on HDFS for yarn.provided.lib.dirs
    val flinkHome = new java.io.File(config.flinkBinPath).getParentFile.getParent
    val hdfsFlinkLibDirs = ensureFlinkLibsOnHdfs(flinkHome)
    // Prepend Flink lib dirs to the additional JAR dirs for yarn.provided.lib.dirs
    val configWithFlinkLibs = stagedConfig.copy(
      flinkJarUris = hdfsFlinkLibDirs ++ stagedConfig.flinkJarUris
    )

    val command = buildCommand(configWithFlinkLibs)
    logger.info(s"Launching Flink Application Mode with command: ${command.mkString(" ")}")

    val (exitCode, yarnAppId) = executeFlinkSubmission(command)

    def cleanup(): Unit = {
      cleanupLocalStaging(localStagingDir)
      cleanupHdfsStaging(hdfsStagingDir)
    }

    if (exitCode != 0 && yarnAppId.isEmpty) {
      logger.error(s"flink run-application failed with exit code $exitCode")
      cleanup()
      System.exit(exitCode)
    }

    yarnAppId match {
      case Some(appId) =>
        logger.info(s"Flink application submitted with YARN application ID: $appId")
        installShutdownHook(appId, localStagingDir, hdfsStagingDir)
        val finalStatus = monitorYarnApplication(appId)
        val code = if (finalStatus == "FINISHED") 0 else 1
        logger.info(s"Flink application $appId terminated with status: $finalStatus")
        cleanup()
        System.exit(code)
      case None =>
        cleanup()
        if (exitCode == 0) {
          logger.warn("flink run-application succeeded but no YARN application ID was captured")
          System.exit(0)
        } else {
          logger.error(s"flink run-application failed with exit code $exitCode and no YARN app ID")
          System.exit(exitCode)
        }
    }
  }

  case class LauncherConfig(
      flinkMainClass: String = "",
      flinkMainJar: String = "",
      flinkJarUris: Seq[String] = Seq.empty,
      flinkCheckpointUri: String = "",
      savepointUri: Option[String] = None,
      flinkProperties: Seq[(String, String)] = Seq.empty,
      flinkArgs: Seq[String] = Seq.empty,
      flinkBinPath: String = DefaultFlinkBinPath
  )

  private[cloud_gcp] def parseArgs(args: Array[String]): LauncherConfig = {
    var config = LauncherConfig()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--flink-main-class" =>
          i += 1; config = config.copy(flinkMainClass = args(i))
        case "--flink-main-jar" =>
          i += 1; config = config.copy(flinkMainJar = args(i))
        case "--flink-jar-uris" =>
          i += 1; config = config.copy(flinkJarUris = args(i).split(",").filter(_.nonEmpty).toSeq)
        case "--flink-checkpoint-uri" =>
          i += 1; config = config.copy(flinkCheckpointUri = args(i))
        case "--savepoint-uri" =>
          i += 1; config = config.copy(savepointUri = Some(args(i)))
        case "--flink-property" =>
          i += 1
          val kv = args(i).split("=", 2)
          if (kv.length == 2) {
            config = config.copy(flinkProperties = config.flinkProperties :+ (kv(0) -> kv(1)))
          }
        case "--flink-arg" =>
          i += 1; config = config.copy(flinkArgs = config.flinkArgs :+ args(i))
        case "--flink-bin-path" =>
          i += 1; config = config.copy(flinkBinPath = args(i))
        case other =>
          logger.warn(s"Unknown launcher argument: $other")
      }
      i += 1
    }

    require(config.flinkMainClass.nonEmpty, "Missing required argument: --flink-main-class")
    require(config.flinkMainJar.nonEmpty, "Missing required argument: --flink-main-jar")
    config
  }

  /** Stages GCS JARs for YARN Application Mode:
    * - Main JAR → local filesystem (flink run-application uploads it to HDFS)
    * - Additional JARs → HDFS directly (referenced via yarn.provided.lib.dirs)
    */
  private[cloud_gcp] def stageGcsJars(config: LauncherConfig,
                                      localStagingDir: String,
                                      hdfsStagingDir: String): LauncherConfig = {
    new java.io.File(localStagingDir).mkdirs()
    runShellCommand("hadoop", "fs", "-mkdir", "-p", hdfsStagingDir)

    // Main JAR to local (flink run-application handles uploading it)
    val stagedMainJar = stageGcsToLocal(config.flinkMainJar, localStagingDir)

    // Additional JARs to HDFS (accessed via yarn.provided.lib.dirs)
    config.flinkJarUris.foreach(uri => stageGcsToHdfs(uri, hdfsStagingDir))

    val hdfsUri = resolveHdfsUri(hdfsStagingDir)
    logger.info(s"Staged main JAR: ${config.flinkMainJar} -> $stagedMainJar")
    logger.info(s"Staged ${config.flinkJarUris.size} additional JARs to HDFS: $hdfsUri")

    // flinkJarUris replaced with the single HDFS staging dir (for yarn.provided.lib.dirs)
    config.copy(flinkMainJar = stagedMainJar, flinkJarUris = Seq(hdfsUri))
  }

  private def stageGcsToLocal(uri: String, localDir: String): String = {
    if (uri.startsWith("gs://")) {
      val fileName = uri.split("/").last
      val localPath = s"$localDir/$fileName"
      logger.info(s"Staging $uri -> $localPath (local)")
      val exitCode = runShellCommand("hadoop", "fs", "-copyToLocal", uri, localPath)
      if (exitCode != 0) {
        throw new RuntimeException(s"Failed to stage $uri to $localPath (exit code $exitCode)")
      }
      localPath
    } else {
      uri
    }
  }

  private def stageGcsToHdfs(uri: String, hdfsDir: String): Unit = {
    if (uri.startsWith("gs://")) {
      val fileName = uri.split("/").last
      val hdfsPath = s"$hdfsDir/$fileName"
      logger.info(s"Staging $uri -> $hdfsPath (HDFS)")
      val exitCode = runShellCommand("hadoop", "fs", "-cp", uri, hdfsPath)
      if (exitCode != 0) {
        throw new RuntimeException(s"Failed to stage $uri to $hdfsPath (exit code $exitCode)")
      }
    }
  }

  /** Ensures Flink lib/ and plugins/ directories exist on HDFS for yarn.provided.lib.dirs.
    * Copies from local Flink installation once, then reuses the HDFS copy.
    */
  private def ensureFlinkLibsOnHdfs(flinkHome: String): Seq[String] = {
    val hdfsBase = s"$HdfsStagingBase/flink-dist"
    val hdfsLib = s"$hdfsBase/lib"
    val hdfsPlugins = s"$hdfsBase/plugins"

    // Check if already staged by looking for flink-dist JAR
    val checkExit = runShellCommand("hadoop", "fs", "-test", "-d", hdfsLib)
    if (checkExit != 0) {
      logger.info(s"Staging Flink distribution to HDFS: $hdfsBase")
      runShellCommand("hadoop", "fs", "-mkdir", "-p", hdfsLib)
      runShellCommand("hadoop", "fs", "-mkdir", "-p", hdfsPlugins)
      runShellCommand("hadoop", "fs", "-put", s"$flinkHome/lib/", hdfsBase)
      runShellCommand("hadoop", "fs", "-put", s"$flinkHome/plugins/", hdfsBase)
    } else {
      logger.info(s"Flink distribution already staged on HDFS: $hdfsBase")
    }

    val resolvedLib = resolveHdfsUri(hdfsLib)
    val resolvedPlugins = resolveHdfsUri(hdfsPlugins)
    Seq(resolvedLib, resolvedPlugins)
  }

  private def resolveHdfsUri(path: String): String = {
    var process: Process = null
    try {
      process = new ProcessBuilder("hdfs", "getconf", "-confKey", "fs.defaultFS")
        .redirectErrorStream(true)
        .start()
      val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
      val output = new StringBuilder
      var line: String = null
      while ({ line = reader.readLine(); line != null }) { output.append(line.trim) }
      process.waitFor()
      val defaultFs = output.toString.trim
      if (defaultFs.startsWith("hdfs://")) s"$defaultFs$path" else s"hdfs://$path"
    } catch {
      case _: Exception => s"hdfs://$path"
    } finally {
      if (process != null) {
        process.getInputStream.close()
        process.destroy()
      }
    }
  }

  private[cloud_gcp] def buildCommand(config: LauncherConfig): Seq[String] = {
    val cmd = ArrayBuffer[String]()
    cmd += config.flinkBinPath
    cmd += "run-application"
    cmd += "-t"
    cmd += "yarn-application"

    // Use parent-first classloader so classes from yarn.provided.lib.dirs and the
    // user JAR are loaded by the same classloader, avoiding ClassCastException.
    cmd += "-Dclassloader.resolve-order=parent-first"

    // Flink properties as -D flags
    config.flinkProperties.foreach { case (k, v) =>
      cmd += s"-D$k=$v"
    }

    // Additional JARs are pre-staged to HDFS. Use yarn.provided.lib.dirs to add them
    // to the YARN container classpath. This property requires Flink's own lib/ and plugins/
    // directories to also be included, so we add the local Flink installation paths
    // (which exist on all Dataproc nodes).
    if (config.flinkJarUris.nonEmpty) {
      val providedDirs = config.flinkJarUris
      cmd += s"-Dyarn.provided.lib.dirs=${providedDirs.mkString(";")}"
    }

    // Savepoint restore
    config.savepointUri.foreach { uri =>
      cmd += "-s"
      cmd += uri
    }

    // Main class
    cmd += "-c"
    cmd += config.flinkMainClass

    // Main JAR (positional argument)
    cmd += config.flinkMainJar

    // Application arguments for FlinkJob.main()
    cmd ++= config.flinkArgs

    cmd.toSeq
  }

  private def executeFlinkSubmission(command: Seq[String]): (Int, Option[String]) = {
    val pb = new ProcessBuilder(command: _*)
    pb.redirectErrorStream(true)

    // Ensure the GCS connector is on the classpath for Hadoop filesystem operations
    val hadoopClasspath = resolveHadoopClasspath()
    if (hadoopClasspath.nonEmpty) {
      val env = pb.environment()
      val existing = Option(env.get("HADOOP_CLASSPATH")).getOrElse("")
      val merged = if (existing.nonEmpty) s"$existing:$hadoopClasspath" else hadoopClasspath
      env.put("HADOOP_CLASSPATH", merged)
      logger.info(s"Set HADOOP_CLASSPATH (${merged.length} chars)")
    }

    val process = pb.start()
    try {
      val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
      var yarnAppId: Option[String] = None
      var line: String = null

      while ({
        line = reader.readLine()
        line != null
      }) {
        logger.info(s"[flink] $line")
        if (yarnAppId.isEmpty) {
          yarnAppId = YarnAppIdPattern.findFirstIn(line)
        }
      }

      val exitCode = process.waitFor()
      (exitCode, yarnAppId)
    } finally {
      process.getInputStream.close()
      process.destroy()
    }
  }

  private def installShutdownHook(yarnAppId: String, localStagingDir: String, hdfsStagingDir: String): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      logger.info(s"Shutdown hook triggered, killing YARN application $yarnAppId")
      var killProcess: Process = null
      try {
        killProcess = new ProcessBuilder("yarn", "application", "-kill", yarnAppId)
          .redirectErrorStream(true)
          .start()
        val killReader = new BufferedReader(new InputStreamReader(killProcess.getInputStream))
        var killLine: String = null
        while ({
          killLine = killReader.readLine()
          killLine != null
        }) {
          logger.info(s"[yarn-kill] $killLine")
        }
        killProcess.waitFor()
      } catch {
        case e: Exception =>
          logger.error(s"Failed to kill YARN application $yarnAppId", e)
      } finally {
        if (killProcess != null) {
          killProcess.getInputStream.close()
          killProcess.destroy()
        }
      }
      cleanupLocalStaging(localStagingDir)
      cleanupHdfsStaging(hdfsStagingDir)
    }))
  }

  private[cloud_gcp] def monitorYarnApplication(yarnAppId: String): String = {
    val terminalStates = Set("FINISHED", "FAILED", "KILLED")

    while (true) {
      val state = getYarnApplicationState(yarnAppId)
      logger.info(s"YARN application $yarnAppId state: $state")
      if (terminalStates.contains(state)) {
        return state
      }
      Thread.sleep(PollIntervalMs)
    }
    "UNKNOWN"
  }

  private def resolveHadoopClasspath(): String = {
    var process: Process = null
    try {
      process = new ProcessBuilder("hadoop", "classpath")
        .redirectErrorStream(true)
        .start()
      val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
      val output = new StringBuilder
      var line: String = null
      while ({
        line = reader.readLine()
        line != null
      }) {
        output.append(line)
      }
      process.waitFor()
      output.toString.trim
    } catch {
      case e: Exception =>
        logger.warn("Failed to resolve hadoop classpath, GCS URIs may not work", e)
        ""
    } finally {
      if (process != null) {
        process.getInputStream.close()
        process.destroy()
      }
    }
  }

  private def cleanupHdfsStaging(stagingDir: String): Unit = {
    try {
      logger.info(s"Cleaning up HDFS staging directory: $stagingDir")
      runShellCommand("hadoop", "fs", "-rm", "-r", "-f", stagingDir)
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to clean up HDFS staging directory $stagingDir", e)
    }
  }

  private def cleanupLocalStaging(stagingDir: String): Unit = {
    try {
      logger.info(s"Cleaning up local staging directory: $stagingDir")
      runShellCommand("rm", "-rf", stagingDir)
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to clean up local staging directory $stagingDir", e)
    }
  }

  private def runShellCommand(command: String*): Int = {
    val process = new ProcessBuilder(command: _*)
      .redirectErrorStream(true)
      .start()
    try {
      val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
      var line: String = null
      while ({
        line = reader.readLine()
        line != null
      }) {
        logger.info(s"[${command.head}] $line")
      }
      process.waitFor()
    } finally {
      process.getInputStream.close()
      process.destroy()
    }
  }

  private[cloud_gcp] def getYarnApplicationState(yarnAppId: String): String = {
    var process: Process = null
    try {
      process = new ProcessBuilder("yarn", "application", "-status", yarnAppId)
        .redirectErrorStream(true)
        .start()
      val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
      var state = "UNKNOWN"
      var line: String = null

      while ({
        line = reader.readLine()
        line != null
      }) {
        line match {
          case YarnStatusStatePattern(s) => state = s.toUpperCase
          case _                         =>
        }
      }
      process.waitFor()
      state
    } catch {
      case e: Exception =>
        logger.error(s"Failed to get YARN application status for $yarnAppId", e)
        "UNKNOWN"
    } finally {
      if (process != null) {
        process.getInputStream.close()
        process.destroy()
      }
    }
  }
}
