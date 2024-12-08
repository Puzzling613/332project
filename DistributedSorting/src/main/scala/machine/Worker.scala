package machine

import io.grpc.{Server, ServerBuilder}
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import com.typesafe.scalalogging.LazyLogging
import scala.collection.mutable.ArrayBuffer
import io.grpc.stub.StreamObserver
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise, ExecutionContext}
import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.util.{Failure, Success, Try}
import scala.jdk.CollectionConverters._
import message._

trait Hyperparams2 {
  val _workerCount: Int = 3 // TODO set count
  val _samplingRate: Double = 0.05
}

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

  val worker = new WorkerServer(masterAddress, workerIp, inputDir, outputDir)
  worker.start()
}

class WorkerService(masterAddress: String, workerIp: String, inputDir: String, outputDir: String) extends WorkerServiceGrpc.WorkerService with LazyLogging with Hyperparams {
  private val channel: ManagedChannel = ManagedChannelBuilder.forTarget(masterAddress).usePlaintext().build()
  private val masterStub: MasterServiceGrpc.MasterServiceBlockingStub = MasterServiceGrpc.blockingStub(channel)

  private var workerId: Int = 0
  private var sendBuffers: Map[Int, Seq[KeyValue]] = Map()
  val receivePromises: Map[Int, Promise[Unit]] = (0 until _workerCount).map { workerId =>
    workerId -> Promise[Unit]()
  }.toMap
  private var receiveBuffers: Map[Int, Seq[KeyValue]] = Map()

  // 문자열을 100글자 단위로 분할
  private val chunks: Iterator[String] = inputDir.grouped(100)
  // 각 조각을 KeyValue 객체로 매핑하여 시퀀스로 변환
  private val input: Seq[KeyValue] = chunks.map { chunk =>
    val key = if (chunk.length >= 10) chunk.substring(0, 10) else chunk
    val value = if (chunk.length > 10) chunk.substring(10) else ""
    KeyValue(key, value)
  }.toSeq
  private var localKeyValue: Seq[KeyValue] = input
  private var partitionBoundaries: Seq[String] = Seq.empty

  def shutdownChannel(): Unit = {
    channel.shutdown()
  }
  def registerWithMaster(): Unit = {
    val request = RegisterWorkerRequest(ip = workerIp)
    val response = Try(masterStub.registerWorker(request))

    response match {
      case Success(reply) =>
        workerId = reply.workerId
        logger.info(s"Registered with Worker ID: ${reply.workerId}")
      case Failure(exception) =>
        logger.error("Failed to registered", exception)
        throw exception
    }
  }
  def safeToDouble(value: String): Double = {
    try {
      value.toDouble
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"Invalid numeric value: $value")
    }
  }

  def samplePhase(): Unit = {
    // get raw data
    val data = localKeyValue

    val samples: ArrayBuffer[Int] = ArrayBuffer[Int]()
    val sampleCount: Int = (_samplingRate * data.length).toInt + 1

    // sort data by key
    val sortedData: Seq[KeyValue] = data.sortBy(_.key)

    // interval calculation
    val minKey: String = sortedData.head.key
    val maxKey: String = sortedData.last.key
    val minKeyNum: Double = safeToDouble(minKey)
    val maxKeyNum: Double = safeToDouble(maxKey)
    val interval: Double = (maxKeyNum - minKeyNum) / sampleCount

    // sample
    for (i <- 0 until sampleCount) {
      val sampleKey: Int = (i * interval).toInt
      samples.append(sampleKey)
    }

    // convert samples to Seq of strings
    val sampled: Seq[String] = samples.map(_.toString).toSeq

    // save samples
    val outputFilePath = Paths.get(inputDir, s"sampled_$workerId")

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

  def sendSampleData(): Unit = {
    val files = Files.list(Paths.get(inputDir)).iterator().asScala.toList.filter(_.toFile.getName.startsWith(s"sample_$workerId"))
    val sampleData = files.flatMap { file =>
      val source = Source.fromFile(file.toFile)
      try source.getLines().take(10).toSeq
      finally source.close()
    }

    val request = GetDataRequest(workerId = workerId, sample = sampleData)
    val response = Try(masterStub.pickBoundariesComplete(request))

    response match {
      case Success(reply) =>
        logger.info(s"Sample data sent. Received partition boundaries: ${reply.partitionBoundaries}")
      case Failure(exception) =>
        logger.error("Failed to send sample data to Master", exception)
        throw exception
    }
  }

  def shufflePhase(): Unit = {
    // partition boundary에 따라 대상 노드를 결정
    def findTargetNode(key: String): Int = {
      val nodeIndex = partitionBoundaries.indexWhere(boundary => key < boundary) // key가 boundaries 중 어느 구간에 있는지 확인
      // 노드 번호 반환
      if (nodeIndex == -1) partitionBoundaries.length // key가 마지막 구간에 속함
      else nodeIndex + 1 // 노드는 1부터 시작한다고 가정
    }
    // 데이터 분할: 로컬 데이터를 partitioning boundary에 따라 분할
    def partitionKeyValue(): Seq[KeyValue] = {
      for (kv <- localKeyValue) {
        val targetNode = findTargetNode(kv.key)
        if (targetNode != workerId) {
          // 해당 노드로 보낼 버퍼에 KeyValue 추가
          sendBuffers = sendBuffers.updated(targetNode, sendBuffers(targetNode) :+ kv)
        }
      }
      // 현재 노드에 남겨야 할 KeyValue들만 필터링하여 반환
      localKeyValue.filter(kv => findTargetNode(kv.key) == workerId)
    }
    //전송
    localKeyValue = partitionKeyValue()
    sendBuffers.foreach { case (targetNode, dataList) =>
      val keys = dataList.map(_.key)
      val values = dataList.map(_.value)
      val request2send = PartitionDataRequest(workerId, partitionBoundaries,keys, values)
      //val responseObserver = new StreamObserver[PartitionDataReply]
      sendPartitionData(request2send) //worker 간 통신
    }
    //Shuffling Complete Log
    val allDataReceived: Future[Unit] = Future.sequence(receivePromises.values.map(_.future)).map(_ => ())
    allDataReceived.onComplete { _ =>
      notifyShuffleComplete()
    }
    println(s"Shuffle Finished")
  }

  override def sendPartitionData(request: PartitionDataRequest): Future[PartitionDataReply] = { //responseObserver: StreamObserver[PartitionDataReply]
    val senderWorkerId = request.senderWorkerId
    val keys = request.keys
    val values = request.values
    val dataList: Seq[KeyValue] = keys.zip(values).map {
      case (key, value) => KeyValue(key, value)
    }
    saveReceivedData(senderWorkerId, dataList)

    val response = PartitionDataReply(success = true)
    //    responseObserver.onNext(response)
    //    responseObserver.w5tonCompleted()
    Future.successful(PartitionDataReply(success = true))
  }
  def saveReceivedData(workerId: Int, dataList: Seq[KeyValue]): Unit = {
    receiveBuffers.get(workerId) match {
      case Some(existingData) =>
        receiveBuffers = receiveBuffers.updated(workerId, existingData ++ dataList)
      case None =>
        receiveBuffers = receiveBuffers + (workerId -> dataList)
    }
    receivePromises(workerId).trySuccess(())
    println(s"Data stored for Worker")
  }

  def notifyShuffleComplete(): Unit = {
    val request = ShuffleCompleteRequest(workerId = workerId)
    val response = Try(masterStub.shuffleComplete(request))

    response match {
      case Success(_) =>
        logger.info("Shuffle complete.")
      case Failure(exception) =>
        logger.error("Failed to notify Shuffle completion to Master.", exception)
        throw exception
    }
  }

  def sortPhase(): Unit = {
    //Implement Sorting
    input.sortBy(_.key)
  }

  def notifySortComplete(): Unit = {
    val request = SortCompleteRequest(workerId = workerId)
    val response = Try(masterStub.sortComplete(request))

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

  def mergePhase(): Unit = {

    var buffers: Map[Int, Seq[KeyValue]] = receiveBuffers.updated(workerId, receiveBuffers(workerId) ++ localKeyValue)
    def mergeBuffers(buffers: Map[Int, Seq[KeyValue]]): Seq[KeyValue] = {
      // 정렬된 두 Seq를 병합하는 함수
      def mergeTwo(seq1: Seq[KeyValue], seq2: Seq[KeyValue]): Seq[KeyValue] = {
        val result = scala.collection.mutable.ListBuffer[KeyValue]()
        var i = 0
        var j = 0

        while (i < seq1.length && j < seq2.length) {
          if (seq1(i).key <= seq2(j).key) {
            result += seq1(i)
            i += 1
          } else {
            result += seq2(j)
            j += 1
          }
        }
        // 남아있는 요소 추가
        result ++= seq1.drop(i)
        result ++= seq2.drop(j)
        result.toSeq
      }
      // 병합 과정을 재귀적으로 수행
      def mergeAll(seqs: Seq[Seq[KeyValue]]): Seq[KeyValue] = {
        if (seqs.isEmpty) Seq.empty
        else if (seqs.length == 1) seqs.head
        else {
          val (left, right) = seqs.splitAt(seqs.length / 2)
          mergeTwo(mergeAll(left), mergeAll(right))
        }
      }
      // buffers의 모든 dataList를 병합
      mergeAll(buffers.values.toSeq)
    }
    val partitionFiles = mergeBuffers(buffers)
    val outputFile = Paths.get(outputDir, s"merged_${workerId}").toFile
    Files.write(outputFile.toPath, partitionFiles.mkString("\n").getBytes)

    logger.info(s"Merge phase completed. Merged data written to: ${outputFile.getPath}")
  }

  def notifyMergeComplete(): Unit = {
    val request = MergeCompleteRequest(workerId = workerId)
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

class WorkerServer(masterAddress: String, workerIp: String, inputDir: String, outputDir: String) extends LazyLogging {
  private val workerService = new WorkerService(masterAddress, workerIp, inputDir, outputDir)
  private val workerServer: Server = ServerBuilder.forPort(7777)
    .addService(WorkerServiceGrpc.bindService(workerService, ExecutionContext.global))
    .build()

  def start(): Unit = {
    try {
      logger.info("Starting WorkerService...")
      workerServer.start()

      logger.info("Registering with Master...")
      workerService.registerWithMaster()

      logger.info("Starting sort phase...")
      workerService.sortPhase()

      logger.info("Notifying Master about sort completion...")
      workerService.notifySortComplete()

      logger.info("Starting sample phase...")
      workerService.samplePhase()

      logger.info("Starting shuffle phase...")
      workerService.shufflePhase()

      logger.info("Notifying Master about shuffle completion...")
      workerService.notifyShuffleComplete()

      logger.info("Starting merge phase...")
      workerService.mergePhase()

      logger.info("Notifying Master about merge completion...")
      workerService.notifyMergeComplete()

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
    workerService.shutdownChannel()  // Shut down the channel
    logger.info("WorkerService shutdown complete.")
  }
}
