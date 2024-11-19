import scala.collection.mutable.ArrayBuffer

// for (key, value) pair
case class KeyValue(key: String, value: String)

// TODO: add appropriate class here
// sample data in each of the workers
def sampleData(data: Seq[KeyValue], p: Double, workerCount: Int): Seq[Int] = {
  val samples = ArrayBuffer[Int]()
  val sampleCount = (p * data.length).toInt

  // Sort data by key
  val sortedData = data.sortBy(_.key)
  val minKey = sortedData.head.key
  val maxKey = sortedData.last.key

  // Interval calculation
  val interval = (maxKey - minKey).toDouble / sampleCount

  // Sampling logic
  for (i <- 0 until sampleCount) {
    val sampleKey = minKey + (i * interval).toInt
    samples.append(sampleKey)
  }

  samples.toSeq
}

// send samples to master using gRPC
def sendSamples(samples: Seq[Int]): Unit = {
  // TODO: implement with network.scala
}
