//package black.worker;
//
//import io.grpc.{Server, ServerBuilder}
//import black.message.{sendData, sortTask}
//import scala.concurrent._
//
//object Worker extends LazyLogging {
//  def main(args: Array[String]): Unit = {
//    implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
//    ArgumentParser.parseWorkerArgs(args) match {
//      case Some(config) =>
//        logger.info(s"Starting worker with configuration: $config")
//        val worker = new Worker(
//          config.masterIP.split(":")(0),
//          config.masterIP.split(":")(1).toInt,
//          config.inputDirectories,
//          config.outputDirectory
//        )(ec)
//        worker.run()
//      case None =>
//        logger.error("Invalid arguments provided.")
//    }
//  }
//}
//
//class Worker(
//              masterHost: String,
//              masterPort: Int,
//              inputDirectories: Seq[String],
//              outputDirectory: String
//            )(implicit ec: ExecutionContext)
//  extends LazyLogging {
//
//  private val channel = ManagedChannelBuilder
//    .forAddress(masterHost, masterPort)
//    .usePlaintext()
//    .asInstanceOf[ManagedChannelBuilder[_]]
//    .build
//  private val stub = MessageGrpc.blockingStub(channel)
//  private var workerID: Option[Int] = None
//  private var totalWorkerCount: Option[Int] = None
//
//  def run(): Unit = {
//    logger.info(s"Connecting to Master at $masterHost:$masterPort")
//    try {
      registerWithMaster()
      processTasks()
//    } catch {
//      case e: Exception =>
//        logger.error(s"Worker encountered an error: ${e.getMessage}")
//        shutDownChannel()
//        System.exit(1)
//    }
//    shutDownChannel()
//  }
//
//  private def shutDownChannel(): Unit = {
//    channel.shutdownNow()
//    logger.info("Channel to master shut down.")
//  }
//
//  private def shufflingTask(): Unit = {
//    //
//    }
//  }
//  private def sortTask(): Unit ={
//    //
//  }
//}
//
//import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
//import scala.concurrent.{ExecutionContext, Future}
//import worker.WorkerGrpc
//import worker.{DistributeStartRequest, DistributeStartResponse, SortStartRequest, SortStartResponse}
//
//object Worker extends App {
//  private val ip: String = "127.0.0.1"
//  private val port: Int = 60051
//  private val masterIp: String = "127.0.0.1"
//  private val masterPort: Int = 50051
//
//  // Master 서버에 연결
//  private val channel = ManagedChannelBuilder
//    .forAddress(masterIp, masterPort)
//    .usePlaintext()
//    .build()
//
//  private val masterStub = MasterGrpc.blockingStub(channel)
//
//  // Master에 등록
//  val registerRequest = RegisterRequest(ip = ip)
//  val registerResponse = masterStub.register(registerRequest)
//  println(s"Registered with Master: ${registerResponse.ip}")
//
//  // Worker 서버 시작
//  private val server = ServerBuilder
//    .forPort(port)
//    .addService(WorkerGrpc.bindService(new WorkerImpl, ExecutionContext.global))  // 생성된 서비스 추가
//    .build
//    .start()
//
//  println(s"Worker started at $ip:$port")
//
//  // 종료 시 서버 클린업
//  sys.addShutdownHook {
//    println("Shutting down worker...")
//    server.shutdown()
//  }
//
//  server.awaitTermination()
//
//  // Worker 서비스 구현
//  private class WorkerImpl extends WorkerGrpc.Worker {
//    override def distributeStart(request: DistributeStartRequest): Future[DistributeStartResponse] = {
//      println(s"Received distribute start request with ranges: ${request.ranges}")
//      Future.successful(DistributeStartResponse())
//    }
//
//    override def sortStart(request: SortStartRequest): Future[SortStartResponse] = {
//      println("Received sort start request")
//      // 정렬 작업 시뮬레이션
//      Thread.sleep(1000)
//      println("Sorting complete")
//      Future.successful(SortStartResponse())
//    }
//  }
//}

      package worker

      import io.grpc.{ManagedChannel, ManagedChannelBuilder, Server, ServerBuilder}
      import worker.WorkerGrpc
      import worker._
      import black.message._
      import com.typesafe.scalalogging.LazyLogging

      import java.nio.file.{Files, Paths}
      import scala.concurrent.{ExecutionContext, Future}
      import scala.io.Source
      import scala.util.{Failure, Success, Try}

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

            logger.info("Starting Shuffle phase...")
            shufflePhase()

            logger.info("Notifying Master about Shuffle completion...")
            notifyShuffleComplete()

            logger.info("Starting Sort phase...")
            sortPhase()

            logger.info("Notifying Master about Sort completion...")
            notifySortComplete()

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

        private def shufflePhase(): Unit = {
          // Master로부터 Key Ranges 수신
          val dataRequest = GetDataRequest()
          val dataResponse = masterStub.getWorkerData(dataRequest)
          val keyRanges = dataResponse.keyRanges

          logger.info(s"Received Key Ranges from Master: $keyRanges")

          // 입력 디렉토리에서 데이터 읽기 및 파티션화
          val files = Files.list(Paths.get(inputDir)).iterator().asScala.toList.filter(_.toFile.isFile)
          val partitions = keyRanges.map(_ => new StringBuilder)

          files.foreach { file =>
            val source = Source.fromFile(file.toFile)
            source.getLines().foreach { line =>
              val key = line.substring(0, 10)
              val partitionIndex = keyRanges.indexWhere(range => key <= range)
              partitions(partitionIndex).append(line).append("\n")
            }
            source.close()
          }

          // 파티션 데이터를 출력 디렉토리에 저장
          partitions.zipWithIndex.foreach { case (data, idx) =>
            val partitionFile = Paths.get(outputDir, s"partition_${workerId.getOrElse(0)}_$idx.txt").toFile
            Files.write(partitionFile.toPath, data.toString.getBytes)
          }

          logger.info("Shuffle phase completed. Partitions written to output directory.")
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

        private def sortPhase(): Unit = {
          // Shuffle로 생성된 파티션 파일 정렬
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
      }
