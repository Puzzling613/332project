import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Success, Failure}
import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable

case class Data(key: String, value: String)

//Worker
object Worker extends App {
  val id: Int = 0
  var localData: List[Data] = Nil
  val partitionBoundaries: List[Int] = Nil
  val totalNodes: Int = 0
  // 각 worker로 전송할 데이터를 저장하는 맵
  var sendBuffers: Map[Int, List[Data]] = partitionData()
  // 수신한 데이터를 저장하는 리스트
  var receiveBuffer: mutable.ListBuffer[Data] = mutable.ListBuffer()

  // 데이터 분할: 로컬 데이터를 partitioning boundary에 따라 분할
  def partitionData(): Map[Int, List[Data]] = {
    for (data <- localData) {
      val targetNode = findTargetNode(data.key)
      if(targetNode) sendBuffers(targetNode) = data :: sendBuffers(targetNode)
    }
    // 보내버린 것 빼고 남김
    localData.filter(data => findTargetNode(data.key) == id)
  }

  // partitioning boundary에 따라 대상 노드를 결정
  def findTargetNode(key: Int): Int = {
    partitionBoundaries.indexWhere(pk => key < pk) match {
      case -1 => partitionBoundaries.length // 마지막 노드
      case idx => idx
    }
  }

  def sendData(): Unit = {
    for ((targetNodeId, dataList) <- sendBuffers) {
      sendToNode(targetNodeId, dataList) //grpc 이용
    }
  }

  def receiveData(dataList: List[Data]): Unit = synchronized {
    //grpc 이용해 받음
    receiveBuffer ++= dataList
  }

  // execute
  def executeShuffleAndMerge(): Unit = {
    //shuffle
    sendData()
    receiveData(dataList)
    // merge
    merge(localData, dataList)
  }
}
