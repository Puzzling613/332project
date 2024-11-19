import scala.collection.mutable.ArrayBuffer

// for (key, value) pair
case class KeyValue(key: String, value: String)

// TODO: add appropriate class here
// pick boundaries on master machine
def pickBoundaries(keys: Seq[Int], workerCount: Int): Seq[Int] = {
  val sortedKeys = keys.sorted
  val boundaries = ArrayBuffer[Int]()

  // pick workerCount-1 boundaries: min, boundary#1, boundary#2, ..., max
  for (i <- 1 until workerCount) {
    boundaries.append(sortedKeys(i * sortedKeys.length / workerCount))
  }

  boundaries.toSeq
}

// send boundaries to workers using gRPC
def sendBoundaries(boundaries: Seq[Int]): Unit = {
  // TODO: implement with network.scala
}

// TODO: add appropriate class here
// partition data based on boundaries in each of the workers
def workerPartition(data: Seq[KeyValue], boundaries: Seq[Int]): Seq[Seq[KeyValue]] = {
  val subsets = ArrayBuffer[ArrayBuffer[KeyValue]]()
  val _boundaries = boundaries :+ Int.MaxValue // added max value to the end

  // initialize empty subsets
  for (_ <- boundaries.indices) {
    subsets.append(ArrayBuffer[KeyValue]())
  }

  // append data to subsets
  for (d <- data) {
    for (i <- boundaries.indices) {
      if (d.key < _boundaries(i)) {
        subsets(i).append(d)
        break
      }
    }
  }

  subsets.map(_.toSeq).toSeq
}
