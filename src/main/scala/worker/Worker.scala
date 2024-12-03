package black.worker

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import worker._
import black.message._
import com.typesafe.scalalogging.LazyLogging

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
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
  val workerIp = java.net.InetAddress.getLocalHost.getHostAddress

  logger.info(s"Starting Worker with Master at $masterAddress")
  logger.info(s"Input Directory: $inputDir, Output Directory: $outputDir")

  val worker = new WorkerService(masterAddress, workerIp, inputDir, outputDir)
  worker.start()
}

class WorkerService(masterAddress: String, workerIp: String, inputDir: String, outputDir: String) extends LazyLogging {
  private val workerServer: Server = ServerBuilder.forPort(7777)
    .addService(new WorkerImpl(this))
    .build()
  private val channel: ManagedChannel = ManagedChannelBuilder.forTarget(masterAddress).usePlaintext().build()
  private val masterStub: MasterGrpc.MasterBlockingStub = MasterGrpc.blockingStub(channel)

  private var workerId: Option[Int] = None
  private var sendBuffers: Map[Int, List[Data]] = Map()
  private var localData: List[Data] = List()

  def start(): Unit = {
    try {
      logger.info("Starting WorkerService...")
      workerServer.start()

      logger.info("Registering with Master...")
      registerWithMaster()

      logger.info("Starting shuffle phase...")
      shufflePhase()

      logger.info("Notifying Master about shuffle completion...")
      notifyShuffleComplete()

      logger.info("Starting sort phase...")
      sortPhase()

      logger.info("Notifying Master about sort completion...")
      notifySortComplete()

      logger.info("Worker tasks completed successfully.")
    } catch {
      case e: Exception =>
        logger.error(s"Worker encountered an error: ${e.getMessage}", e)
    } finally {
      shutdown()
    }
  }

  def shutdown(): Unit = {
    logger.info("Shutting down WorkerService...")
    workerServer.shutdown()
    channel.shutdown()
    logger.info("WorkerService shutdown complete.")
  }

  private def registerWithMaster(): Unit = {
    val request = RegisterWorkerRequest(ip = workerIp)
    val response = Try(masterStub.registerWorker(request))

    response match {
      case Success(reply) =>
        workerId = Some(reply.workerId)
        logger.info(s"Registered with Worker ID: ${reply.workerId}")
      case Failure(exception) =>
        logger.error("Failed to registered", exception)
        throw exception
    }
  }

  def shufflePhase(): Unit = {
    //*TODO* Implement shuffling
    sendBuffers.foreach { case (targetNode, dataList) =>
      sendPartitionData(targetNode, dataList)
    }
    //*TODO* Shuffling Complete Log
  }

  def sendPartitionData(targetNode: Int, dataList: List[Data]): Unit = {
    val targetWorkerIp = getWorkerIp(targetNode)
    val channel = ManagedChannelBuilder.forTarget(s"$targetWorkerIp:7777")
      .usePlaintext()
      .build()
    val stub = WorkerGrpc.blockingStub(channel)

    val request = PartitionDataRequest(
      senderWorkerId = workerId.getOrElse(0),
      dataList = dataList.map(data => DataMessage(data.key, data.value)).asJava
    )

    Try(stub.sendPartitionData(request)) match {
      case Success(reply) if reply.success =>
        logger.info(s"Successfully sent data to Worker.")
      case Failure(exception) =>
        logger.error(s"Failed to send data to Worker.", exception)
    } finally {
      channel.shutdown()
    }
  }

  private def notifyShuffleComplete(): Unit = {
    val request = ShuffleCompleteRequest(workerId = workerId.getOrElse(0))
    val response = Try(masterStub.shuffleComplete(request))

    response match {
      case Success(_) =>
        logger.info("Shuffle complete.")
      case Failure(exception) =>
        logger.error("Failed to notify Shuffle completion to Master.", exception)
        throw exception
    }
  }

  private def sortPhase(): Unit = {
    //*TODO* Implement Sorting
  }

  private def notifySortComplete(): Unit = {
    val request = MergeSortCompleteRequest(workerId = workerId.getOrElse(0))
    val response = Try(masterStub.mergeSortComplete(request))

    response match {
      case Success(_) =>
        logger.info("Sort complete.")
      case Failure(exception) =>
        logger.error("Failed to notify Sort Completion to Master", exception)
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
