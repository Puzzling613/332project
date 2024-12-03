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
