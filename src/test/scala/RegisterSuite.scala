import io.grpc.{ManagedChannel, ManagedChannelBuilder, Server, ServerBuilder}
import scala.concurrent.ExecutionContext
import worker._
import master._

// Master test server
object TestMaster extends App {
  val server: Server = ServerBuilder.forPort(7777)
    .addService(new TestMasterServiceImpl())
    .build()

  println("Starting Test Master Server on port 7777...")
  server.start()
  println("Test Master Server is running.")
  server.awaitTermination()
}

class TestMasterServiceImpl extends MasterGrpc.MasterImplBase {
  override def registerWorker(request: RegisterWorkerRequest, responseObserver: io.grpc.stub.StreamObserver[RegisterWorkerReply]): Unit = {
    println(s"Received worker registration request from ${request.ip}")
    val reply = RegisterWorkerReply(workerId = 1) // Assign a test worker ID
    responseObserver.onNext(reply)
    responseObserver.onCompleted()
    println("Sent worker registration response.")
  }

  override def shuffleComplete(request: ShuffleCompleteRequest, responseObserver: io.grpc.stub.StreamObserver[ShuffleCompleteReply]): Unit = {
    println(s"Received shuffle complete notification from Worker ID: ${request.workerId}")
    val reply = ShuffleCompleteReply(success = true)
    responseObserver.onNext(reply)
    responseObserver.onCompleted()
    println("Acknowledged shuffle completion.")
  }
}

// Worker server
object TestWorker extends App {
  val channel: ManagedChannel = ManagedChannelBuilder.forTarget("localhost:7777").usePlaintext().build()
  val masterStub: MasterGrpc.MasterBlockingStub = MasterGrpc.blockingStub(channel)

  try {
    // Step 1: Register with Master
    println("Registering Worker with Master...")
    val registerRequest = RegisterWorkerRequest(ip = "ip뭐지")
    val registerResponse = masterStub.registerWorker(registerRequest)
    println(s"Received Worker ID from Master: ${registerResponse.workerId}")

    // Step 2: Notify shuffle completion
    println("Notifying Master of shuffle completion...")
    val shuffleCompleteRequest = ShuffleCompleteRequest(workerId = registerResponse.workerId)
    val shuffleCompleteResponse = masterStub.shuffleComplete(shuffleCompleteRequest)
    println(s"Shuffle completion acknowledged: ${shuffleCompleteResponse.success}")
  } catch {
    case e: Exception =>
      println(s"Error during communication with Master: ${e.getMessage}")
  } finally {
    channel.shutdown()
  }
}
