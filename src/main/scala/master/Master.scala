package black.master

import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.concurrent.TrieMap
import scala.util.Sorting
import java.util.concurrent.atomic.AtomicInteger
import com.typesafe.scalalogging.LazyLogging
import black.message._

case class KeyValue(key: String, value: String)

trait Hyperparams {
  val _workerCount = Nil
}

object MasterApp extends App with LazyLogging with Hyperparams {
  def main(args: Array[String]): Unit = {
    val port = 7777
    logger.info(s"Starting Master server on port $port")
    val masterServer = new MasterServer(port, _workerCount)
    masterServer.start()
    masterServer.blockUntilShutdown()
  }
}

class MasterServer(port: Int, numWorkers: Int) extends LazyLogging {
  private var server: Server = _
  private val service = new MasterService(numWorkers)

  def start(): Unit = {
    server = ServerBuilder
      .forPort(port)
      .addService(MasterService.bindService(service, ExecutionContext.global))
      .build()
      .start()
    logger.info(s"Master server started on port $port ^_^")
    sys.addShutdownHook {
      logger.warn("Shutting down Master server o_o")
      stop()
      logger.warn("Master server shut down ㅠ.ㅠ")
    }
  }

  def blockUntilShutdown(): Unit = {
    server.awaitTermination()
  }

  def stop(): Unit = {
    if (server != null) server.shutdown()
  }
}

class MasterService(numWorkers: Int) with LazyLogging {
  private val workers = TrieMap[Int, String]()
  private val workerIdCounter = new AtomicInteger(0)
  private var shuffleCompletedWorkers = Set.empty[Int]
  private var mergeCompletedWorkers = Set.empty[Int]
  private var samples = List.empty[Seq[KeyValue]]
  private var partitionBoundaries = Seq.empty[String]

  override def registerWorker(request: RegisterWorkerRequest): Future[RegisterWorkerReply] = {
    val workerId = workerIdCounter.incrementAndGet()
    workers.put(workerId, request.ip)
    logger.info(s"Worker registered: ID=$workerId, IP=${request.ip}")

    if (workers.size == numWorkers) {
      logger.info("All workers registered.")
    }

    Future.successful(RegisterWorkerReply(workerId = workerId))
  }

  override def PickBoundariesComplete(request: WorkerDataRequest): Future[WorkerDataReply] = {
    samples :+= request.sample
    logger.info(s"Sample data received from Worker ID: ${request.workerId}")

    if (samples.size == numWorkers) {
      val sortedSamples = samples.flatten.sorted
      val partitionSize = sortedSamples.length / numWorkers
      // pick numWorkers-1 boundaries
      partitionBoundaries = (1 until numWorkers).map(i => sortedSamples(i * partitionSize))
      logger.info(s"Partition boundaries calculated: $partitionBoundaries")
      workers.keys.foreach { workerId =>
        logger.info(s"Sending partition boundaries to Worker ID: $workerId")
      }
    }

    Future.successful(WorkerDataReply(partitionBoundaries = partitionBoundaries))
  }

  override def shuffleStart(request: ShuffleRequest): Future[ShuffleReply] = {
    logger.info("Shuffle started for all workers")
    Future.successful(ShuffleReply(success = true))
  }

  override def shuffleComplete(request: ShuffleCompleteRequest): Future[ShuffleCompleteReply] = {
    shuffleCompletedWorkers += request.workerId
    logger.info(s"Shuffle completed by Worker ID: ${request.workerId}")

    if (shuffleCompletedWorkers.size == numWorkers) {
      logger.info("All workers completed Shuffle. Starting Merge phase...")
      workers.keys.foreach { workerId =>
        logger.info(s"Notifying Worker ID: $workerId to start merging.")
      }
    }

    Future.successful(ShuffleCompleteReply(success = true))
  }

  override def merge(request: MergeRequest): Future[MergeReply] = {
    logger.info(s"Merge started for Worker ID: ${request.workerId}")
    Future.successful(MergeReply(success = true))
  }

  override def mergeComplete(request: MergeCompleteRequest): Future[MergeCompleteReply] = {
    mergeCompletedWorkers += request.workerId
    logger.info(s"Merge completed by Worker ID: ${request.workerId}")

    if (mergeCompletedWorkers.size == numWorkers) {
      logger.info("All workers completed Merge. Distributed Sorting completed successfully.")
    }

    Future.successful(MergeCompleteReply(success = true))
  }
}
