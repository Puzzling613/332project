package black.worker

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import worker.WorkerGrpc
import worker._
import black.message._
import com.typesafe.scalalogging.LazyLogging

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.io.Source
import scala.util.{Failure, Success, Try}

case class KeyValue(key: String, value: String)

object WorkerApp extends App with LazyLogging {

  if (args.length < 4 || !args.contains("-I") || !args.contains("-O")) {
    println("Usage: Worker <Master IP:Port> -I <input directory> -O <output directory>")
    System.exit(1)
  }

  val masterAddress = args(0)
  val inputDir = args(args.indexOf("-I") + 1)
  val outputDir = args(args.indexOf("-O") + 1)

  logger.info(s"Starting Worker with Master at $masterAddress")
  logger.info(s"Input Directory: $inputDir, Output Directory: $outputDir")

  val worker = new WorkerService(masterAddress, inputDir, outputDir)
  worker.start()
}

class WorkerService(masterAddress: String, inputDir: String, outputDir: String) extends LazyLogging {

  // gRPC 채널 설정
  private val channel: ManagedChannel = ManagedChannelBuilder.forTarget(masterAddress).usePlaintext().build()
  private val masterStub: MasterGrpc.MasterBlockingStub = MasterGrpc.blockingStub(channel)
  private implicit val ec: ExecutionContext = ExecutionContext.global

  private var workerId: Option[Int] = None

  def start(): Unit = {
    try {
      logger.info("Registering with Master...")
      registerWithMaster()

      logger.info("Sending sample data to Master...")
      sendSampleData()

      logger.info("Starting Shuffle phase...")
      shufflePhase()

      logger.info("Notifying Master about Shuffle completion...")
      notifyShuffleComplete()

      logger.info("Waiting for Sort instruction from Master...")
      waitSort()

      logger.info("Starting Sort phase...")
      sortPhase()

      logger.info("Notifying Master about Sort completion...")
      notifySortComplete()

      logger.info("Waiting for Merge instruction from Master...")
      waitMerge()

      logger.info("Starting Merge phase...")
      mergePhase()

      logger.info("Notifying Master about Merge completion...")
      notifyMergeComplete()

    } catch {
      case e: Exception =>
        logger.error(s"Worker encountered an error: ${e.getMessage}", e)
        channel.shutdown()
        System.exit(1)
    }

    // Clean up
    channel.shutdown()
    logger.info("Worker tasks completed successfully.")
  }

  private def registerWithMaster(): Unit = {
    val workerIp = java.net.InetAddress.getLocalHost.getHostAddress
    val request = RegisterWorkerRequest(ip = workerIp)
    val response = Try(masterStub.registerWorker(request))

    response match {
      case Success(reply) =>
        workerId = Some(reply.workerId)
        logger.info(s"Registered with Master. Assigned Worker ID: ${reply.workerId}")
      case Failure(exception) =>
        logger.error("Failed to register with Master", exception)
        throw exception
    }
  }

  private def sendSampleData(): Unit = {
    val files = Files.list(Paths.get(inputDir)).iterator().asScala.toList.filter(_.toFile.isFile)
    val sampleData = files.flatMap { file =>
      val source = Source.fromFile(file.toFile)
      try source.getLines().take(10).toSeq
      finally source.close()
    }

    val request = WorkerDataRequest(workerId = workerId.getOrElse(0), sample = sampleData)
    val response = Try(masterStub.getWorkerData(request))

    response match {
      case Success(reply) =>
        logger.info(s"Sample data sent. Received partition boundaries: ${reply.partitionBoundaries}")
      case Failure(exception) =>
        logger.error("Failed to send sample data to Master", exception)
        throw exception
    }
  }

  private def shufflePhase(): Unit = {
    // Shuffle phase logic
    val partitionFiles = partitionData()
    logger.info(s"Partitions created: ${partitionFiles.map(_.getFileName)}")
  }

  private def partitionData(): Seq[Paths] = {
    val data = Files.list(Paths.get(inputDir)).iterator().asScala.flatMap { path =>
      val source = Source.fromFile(path.toFile)
      try source.getLines()
      finally source.close()
    }

    val partitions = Seq.newBuilder[Seq[String]]
    // Partition data logic
    partitions.result()
  }

  private def notifyShuffleComplete(): Unit = {
    val request = ShuffleCompleteRequest(workerId = workerId.getOrElse(0))
    val response = Try(masterStub.shuffleComplete(request))

    response match {
      case Success(_) =>
        logger.info("Notified Master about Shuffle completion.")
      case Failure(exception) =>
        logger.error("Failed to notify Master about Shuffle completion", exception)
        throw exception
    }
  }

  private def waitSort(): Unit = {
    // Block until Master gives Sort instruction
    Thread.sleep(1000) // Placeholder for waiting logic
  }

  private def sortPhase(): Unit = {
    val partitionFiles = Files.list(Paths.get(outputDir)).iterator().asScala.toList
      .filter(_.toFile.getName.startsWith(s"partition_${workerId.getOrElse(0)}"))

    partitionFiles.foreach { file =>
      val lines = Source.fromFile(file.toFile).getLines().toSeq.sorted
      Files.write(file, lines.mkString("\n").getBytes)
    }

    logger.info("Sort phase completed. Partitions sorted.")
  }

  private def notifySortComplete(): Unit = {
    val request = MergeSortCompleteRequest(workerId = workerId.getOrElse(0))
    val response = Try(masterStub.mergeSortComplete(request))

    response match {
      case Success(_) =>
        logger.info("Notified Master about Sort completion.")
      case Failure(exception) =>
        logger.error("Failed to notify Master about Sort completion", exception)
        throw exception
    }
  }

  private def waitMerge(): Unit = {
    // Block until Master gives Merge instruction
    Thread.sleep(1000) // Placeholder for waiting logic
  }

  private def mergePhase(): Unit = {
    val partitionFiles = Files.list(Paths.get(outputDir)).iterator().asScala.toList
      .filter(_.toFile.getName.startsWith(s"partition_${workerId.getOrElse(0)}"))

    val mergedData = partitionFiles.flatMap { file =>
      val source = Source.fromFile(file.toFile)
      try source.getLines()
      finally source.close()
    }.sorted

    val outputFile = Paths.get(outputDir, s"merged_${workerId.getOrElse(0)}.txt").toFile
    Files.write(outputFile.toPath, mergedData.mkString("\n").getBytes)

    logger.info(s"Merge phase completed. Merged data written to: ${outputFile.getPath}")
  }

  private def notifyMergeComplete(): Unit = {
    val request = MergeCompleteRequest(workerId = workerId.getOrElse(0))
    val response = Try(masterStub.mergeComplete(request))

    response match {
      case Success(_) =>
        logger.info("Notified Master about Merge completion.")
      case Failure(exception) =>
        logger.error("Failed to notify Master about Merge completion", exception)
        throw exception
    }
  }
}
