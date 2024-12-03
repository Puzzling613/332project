package black.worker

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import worker._
import black.message._
import com.typesafe.scalalogging.LazyLogging
import scala.collection.mutable.ArrayBuffer
import io.grpc.stub.StreamObserver
import scala.concurrent.{Future, ExecutionContext}
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
  val input = args(args.indexOf("-I") + 1)
  // 문자열을 100글자 단위로 분할
  val chunks: Iterator[String] = input.grouped(100)
  // 각 조각을 KeyValue 객체로 매핑하여 시퀀스로 변환
  val inputDir: Seq[KeyValue] = chunks.map { chunk =>
    val key = if (chunk.length >= 10) chunk.substring(0, 10) else chunk
    val value = if (chunk.length > 10) chunk.substring(10) else ""
    KeyValue(key, value)
  }.toSeq
  val outputDir = args(args.indexOf("-O") + 1)
  val workerIp = java.net.InetAddress.getLocalHost.getHostAddress

  logger.info(s"Starting Worker with Master at $masterAddress")
  logger.info(s"Input Directory: $inputDir, Output Directory: $outputDir")

  val worker = new WorkerService(masterAddress, workerIp, inputDir, outputDir)
  worker.start()
}

class WorkerServer(masterAddress: String, workerIp: String, inputDir: String, outputDir: String) extends LazyLogging {
  private val workerServer: Server = ServerBuilder.forPort(7777)
    .addService(new WorkerService(this))
    .build()
  def start(): Unit = {
    try {
      logger.info("Starting WorkerService...")
      workerServer.start()

      logger.info("Registering with Master...")
      registerWithMaster()

      logger.info("Starting sample phase...")
      samplePhase()

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
}
class WorkerService(masterAddress: String, workerIp: String, inputDir: String, outputDir: String) extends LazyLogging {
  private val channel: ManagedChannel = ManagedChannelBuilder.forTarget(masterAddress).usePlaintext().build()
  private val masterStub: MasterGrpc.MasterBlockingStub = MasterGrpc.blockingStub(channel)

  private var workerId: Option[Int] = None
  private var sendBuffers: Map[Int, Seq[KeyValue]] = Map()
  private var receiveBuffers: Map[Int, Seq[KeyValue]] = Map()
  private var localData: List[Data] = List()

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

  def samplePhase(): Unit = {
    // get raw data
    val files = Files.list(Paths.get(outputDir)).iterator().asScala.toList
      .filter(_.toFile.getName.startsWith(s"gensort_${workerId.getOrElse(0)}")) // TODO replace with real files
    val data = files.flatMap { file =>
      val source = Source.fromFile(file.toFile)
      try source.getLines()
      finally source.close()
    }.sorted

    val samples = ArrayBuffer[Int]()
    val sampleCount = (p * data.length).toInt

    // sort data by key
    val sortedData = data.sortBy(_.key)

    // interval calculation
    val minKey = sortedData.head.key
    val maxKey = sortedData.last.key
    val interval = (maxKey - minKey).toDouble / sampleCount

    // sample
    for (i <- 0 until sampleCount) {
      val sampleKey = (i * interval).toInt
      samples.append(sampleKey)
    }

    // convert samples to Seq of strings
    val sampled = samples.map(_.toString)

    // save sampmles
    val outputFilePath = Paths.get(outputDir, s"sampled_$workerId")

    // write the sample strings to the file
    try {
      Files.write(outputFilePath, sampled.mkString("\n").getBytes)
      logger.info(s"Samples saved to ${outputFilePath.toString}")
    } catch {
      case e: Exception =>
        logger.error(s"Error saving samples to file: ${e.getMessage}", e)
        throw e
    }
  }

  private def sendSampleData(): Unit = {
    val files = Files.list(Paths.get(inputDir)).iterator().asScala.toList.filter(_.toFile.getName.startsWith(s"sample_$workerId"))
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

  def shufflePhase(): Unit = {
    val localKeyValue: Seq[KeyValue] = inputDir
    val partitionBoundaries: Seq[String] = ???
    // partition boundary에 따라 대상 노드를 결정
    def findTargetNode(key: String): String = {
      partitionBoundaries.indexWhere(pk => key < pk) match {
        case -1 => partitionBoundaries.length.toString // 마지막 노드의 ID로 설정
        case idx => idx.toString // 해당 노드의 ID로 설정
      }
    }

    // 데이터 분할: 로컬 데이터를 partitioning boundary에 따라 분할
    def partitionKeyValue(): Seq[KeyValue] = {
      for (kv <- localKeyValue) {
        val targetNode = findTargetNode(kv.key)
        if (targetNode != id) {
          // 해당 노드로 보낼 버퍼에 KeyValue 추가
          sendBuffers = sendBuffers.updated(targetNode, sendBuffers(targetNode) :+ kv)
        }
      }
      // 현재 노드에 남겨야 할 KeyValue들만 필터링하여 반환
      localKeyValue.filter(kv => findTargetNode(kv.key) == id)
    }
    sendBuffers.foreach { case (targetNode, dataList) =>
      sendPartitionData(targetNode, dataList) //worker 간 통신
    }
    //*TODO* Shuffling Complete Log
  }

  override def sendPartitionData(request: PartitionDataRequest, responseObserver: StreamObserver[PartitionDataResponse]): Unit = {
    val senderWorkerId = request.senderWorkerId
    val dataList = request.dataList.asScala.toList.map(dataMessage => KeyValue(dataMessage.key, dataMessage.value))

    saveReceivedData(senderWorkerId, dataList)

    val response = PartitionDataResponse(success = true)
    responseObserver.onNext(response)
    responseObserver.onCompleted()

    processReceivedData()
  }

  private def saveReceivedData(workerId: Int, dataList: Seq[KeyValue]): Unit = {
    receiveBuffers.get(workerId) match {
      case Some(existingData) =>
        receiveBuffers = receiveBuffers.updated(workerId, existingData ++ dataList)
      case None =>
        receiveBuffers = receiveBuffers + (workerId -> dataList)
    }
    println(s"Data stored for Worker")
  }

  private def processReceivedData(): Unit = {
  //어쩌고저쩌고 merge하고 어쩌고 저쩌고 ㄱㄱ
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
