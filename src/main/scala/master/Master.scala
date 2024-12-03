package black.master

import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.concurrent.TrieMap
import java.util.concurrent.atomic.AtomicInteger
import com.typesafe.scalalogging.LazyLogging
import black.message._

object MasterApp extends App with LazyLogging {
  def main(args: Array[String]): Unit = {
    val port = 7777
    val Workers = 3
    logger.info(s"Starting Master server on port $port")
    val masterServer = new MasterServer(port, Workers)
    masterServer.start()
    masterServer.blockUntilShutdown()
  }
}

class MasterServer(port: Int, expectedWorkers: Int) extends LazyLogging {
  private var server: Server = _
  private val service = new MasterService(expectedWorkers)

  def start(): Unit = {
    server = ServerBuilder
      .forPort(port)
      .addService(SortingServiceGrpc.bindService(service, ExecutionContext.global))
      .build()
      .start()
    logger.info(s"Master server started on port $port")
    sys.addShutdownHook {
      logger.warn("Shutting down Master server...")
      stop()
      logger.warn("Master server shut down.")
    }
  }

  def blockUntilShutdown(): Unit = {
    server.awaitTermination()
  }

  def stop(): Unit = {
    if (server != null) server.shutdown()
  }
}

class MasterService(expectedWorkers: Int) extends SortingServiceGrpc.SortingService with LazyLogging {
  private val workers = TrieMap[Int, String]() // Worker ID -> IP
  private val workerIdCounter = new AtomicInteger(0)
  private var shuffleCompletedWorkers = Set.empty[Int]
  private var mergeCompletedWorkers = Set.empty[Int]

  override def registerWorker(request: RegisterWorkerRequest): Future[RegisterWorkerReply] = {
    val workerId = workerIdCounter.incrementAndGet()
    workers.put(workerId, request.ip)
    logger.info(s"Worker registered: ID=$workerId, IP=${request.ip}")

    if (workers.size == expectedWorkers) {
      logger.info("All workers registered.")
    }

    Future.successful(RegisterWorkerReply(workerId = workerId))
  }

  override def shuffleStart(request: ShuffleRequest): Future[ShuffleReply] = {
    logger.info("Shuffle started for all workers")
    // Shuffle 시작 로직
    Future.successful(ShuffleReply(success = true))
  }

  override def shuffleComplete(request: ShuffleCompleteRequest): Future[ShuffleCompleteReply] = {
    shuffleCompletedWorkers += request.workerId
    logger.info(s"Shuffle completed by Worker ID: ${request.workerId}")

    if (shuffleCompletedWorkers.size == expectedWorkers) {
      logger.info("All workers completed Shuffle.")
    }

    Future.successful(ShuffleCompleteReply(success = true))
  }

  override def merge(request: MergeRequest): Future[MergeReply] = {
    logger.info(s"Merge started for Worker ID: ${request.workerId}")
    // Merge 작업 시작 로직
    Future.successful(MergeReply(success = true))
  }

  override def mergeComplete(request: MergeCompleteRequest): Future[MergeCompleteReply] = {
    mergeCompletedWorkers += request.workerId
    logger.info(s"Merge completed by Worker ID: ${request.workerId}")

    if (mergeCompletedWorkers.size == expectedWorkers) {
      logger.info("All workers completed Merge.")
    }

    Future.successful(MergeCompleteReply(success = true))
  }
}
