# shuffle/merge  
두 과정 모두 각 worker에서 이루어지기 때문에 worker에서를 전제로 생각한다.

## Shuffle  
partition 과정에서 얻은 키를 바탕으로 범위 안의 것은 가지고 범위 밖의 것은 해당 범위에 해당하는 worker machine으로 보낸다.  

정렬된 로컬데이터를 partition key에 따라 자른다. 그리고 grpc를 이용해서 각 worker로 전송을 한다. 그리고 worker로부터 오는 데이터들을 수신한다. 이는 synchronous하기 때문에 grpc를 잘 해야 한다. 

### psuedo code
```python
# Node 클래스는 각 노드를 나타냅니다.
class Node:
    id              # 노드의 식별자
    local_data      # 로컬에서 정렬된 데이터 리스트
    partition_keys  # 파티셔닝 키 리스트
    send_buffers    # 각 노드로 전송할 데이터를 저장하는 딕셔너리
    receive_buffers # 수신한 데이터를 저장하는 딕셔너리

    # 초기화 메서드

    # 데이터 분할 메서드
    partition_data():
        for data in self.local_data:
            target_node = find_target_node(data.key)
            if target_node == self.id:
                # 해당 데이터는 자신이 처리하므로 그대로 유지
                continue
            else:
                # 전송할 버퍼에 데이터 추가
                if target_node not in self.send_buffers:
                    self.send_buffers[target_node] = []
                self.send_buffers[target_node].append(data)
        # 로컬 데이터에서 전송할 데이터를 제거
        self.local_data.remove(data)

    # 대상 노드 결정 함수
    find_target_node(key):
        for i in range(len(self.partition_keys)):
            if key < self.partition_keys[i]:
                return i    # 노드 ID가 0부터 시작한다고 가정
        return len(self.partition_keys)  # 마지막 노드

    # 데이터 전송 메서드 (gRPC 사용)
    send_data():
        for target_node, data_list in self.send_buffers.items():
            grpc_send(target_node, data_list)

    # 데이터 수신 메서드 (gRPC 사용)
    receive_data(source_node, data_list):
        if source_node not in self.receive_buffers:
            self.receive_buffers[source_node] = []
        self.receive_buffers[source_node].extend(data_list)

    # 셔플 과정 실행 메서드
    execute_shuffle():
        partition_data()
        send_data()
        wait_for_all_data()
        merge_data()

    # 모든 데이터 수신 대기
    wait_for_all_data():
        # 모든 노드로부터 데이터를 수신할 때까지 대기
        while not all_data_received():
            sleep(1)   # 잠시 대기 후 다시 확인

    # 모든 데이터 수신 여부 확인
    all_data_received():
        # 구현 방식에 따라 확인 (예: 수신해야 할 데이터 크기 등을 알고 있는 경우)
        return True    # 간단히 모든 데이터가 수신되었다고 가정

```

```scala
// 데이터 요소를 나타내는 클래스
case class Data(key: Int, value: Any)

// Node 클래스는 각 노드를 나타냅니다.
class Node(
  val id: Int,
  var localData: List[Data],
  val partitionKeys: List[Int]
) {
  // 각 노드로 전송할 데이터를 저장하는 맵
  val sendBuffers: scala.collection.mutable.Map[Int, List[Data]] = scala.collection.mutable.Map()
  // 수신한 데이터를 저장하는 맵
  val receiveBuffers: scala.collection.mutable.Map[Int, List[Data]] = scala.collection.mutable.Map()

  // 데이터 분할 메서드
  def partitionData(): Unit = {
    // 전송할 데이터를 임시로 저장할 맵 초기화
    partitionKeys.indices.foreach { nodeId =>
      sendBuffers(nodeId) = List()
    }

    // 로컬 데이터를 파티셔닝 키에 따라 분할
    localData.foreach { data =>
      val targetNode = findTargetNode(data.key)
      if (targetNode != id) {
        // 전송할 버퍼에 데이터 추가
        sendBuffers(targetNode) = data :: sendBuffers(targetNode)
      }
    }

    // 로컬 데이터에서 전송할 데이터를 제거
    localData = localData.filter(data => findTargetNode(data.key) == id)
  }

  // 대상 노드 결정 함수
  def findTargetNode(key: Int): Int = {
    partitionKeys.indexWhere(pk => key < pk) match {
      case -1 => partitionKeys.length // 마지막 노드
      case idx => idx
    }
  }

  // 데이터 전송 메서드 (gRPC 사용)
  def sendData(): Unit = {
    sendBuffers.foreach { case (targetNodeId, dataList) =>
      grpcSend(targetNodeId, dataList)
    }
  }

  // 데이터 수신 메서드 (gRPC 사용)
  def receiveData(sourceNodeId: Int, dataList: List[Data]): Unit = {
    val existingData = receiveBuffers.getOrElse(sourceNodeId, List())
    receiveBuffers(sourceNodeId) = dataList ++ existingData
  }

  // 셔플 과정 실행 메서드
  def executeShuffle(): Unit = {
    partitionData()
    sendData()
    waitForAllData()
    mergeData()
  }

  // 모든 데이터 수신 대기
  def waitForAllData(): Unit = {
    // 모든 노드로부터 데이터를 수신할 때까지 대기
    while (!allDataReceived()) {
      Thread.sleep(100) // 잠시 대기 후 다시 확인
    }
  }

  // 모든 데이터 수신 여부 확인
  def allDataReceived(): Boolean = {
    // 수신해야 할 노드 수를 알고 있다고 가정
    val expectedNodes = partitionKeys.length + 1 // 노드 수
    receiveBuffers.size == expectedNodes - 1 // 자기 자신 제외
  }

  // 데이터 병합 메서드
  def mergeData(): Unit = {
    // 수신한 데이터와 로컬 데이터를 합침
    receiveBuffers.values.foreach { dataList =>
      localData = dataList ++ localData
    }
    // 최종 정렬 수행
    localData = mergeSort(localData)
  }

  // 병합 정렬 함수
  def mergeSort(dataList: List[Data]): List[Data] = {
    if (dataList.length <= 1) {
      dataList
    } else {
      val mid = dataList.length / 2
      val left = mergeSort(dataList.take(mid))
      val right = mergeSort(dataList.drop(mid))
      merge(left, right)
    }
  }

  // 두 리스트 병합 함수
  def merge(left: List[Data], right: List[Data]): List[Data] = {
    (left, right) match {
      case (Nil, _) => right
      case (_, Nil) => left
      case (lHead :: lTail, rHead :: rTail) =>
        if (lHead.key <= rHead.key) {
          lHead :: merge(lTail, right)
        } else {
          rHead :: merge(left, rTail)
        }
    }
  }
}

// gRPC를 사용한 데이터 전송 함수 (클라이언트 측)
def grpcSend(targetNodeId: Int, dataList: List[Data]): Unit = {
  // gRPC 클라이언트를 통해 대상 노드로 데이터 전송
  val grpcClient = grpcConnect(targetNodeId)
  grpcClient.sendData(dataList)
}

// gRPC 서버에서 데이터 수신 핸들러 (서버 측)
def onDataReceived(sourceNodeId: Int, dataList: List[Data]): Unit = {
  currentNode.receiveData(sourceNodeId, dataList)
}

// 노드 실행 예시
object DistributedSort {
  def main(args: Array[String]): Unit = {
    val numNodes = 4 // 노드 수
    val partitionKeys = getPartitionKeys() // 파티셔닝 키를 가져옴

    // 각 노드 초기화
    val nodes = (0 until numNodes).map { i =>
      val localData = loadLocalData(i) // 로컬 데이터를 로드
      new Node(i, localData, partitionKeys)
    }

    // 각 노드에서 셔플 과정 실행
    nodes.foreach { node =>
      node.executeShuffle()
    }
  }

  // 파티셔닝 키를 가져오는 함수 (예시)
  def getPartitionKeys(): List[Int] = {
    // 샘플링 결과로부터 파티셔닝 키를 생성했다고 가정
    List(25, 50, 75) // 예시 값
  }

  // 로컬 데이터를 로드하는 함수 (예시)
  def loadLocalData(nodeId: Int): List[Data] = {
    // 각 노드의 로컬 데이터를 생성하거나 로드
    // 예시로 임의의 데이터를 생성
    (1 to 100).map { i =>
      Data(key = scala.util.Random.nextInt(100), value = s"Value $i at Node $nodeId")
    }.toList
  }

  // 현재 노드를 나타내는 변수 (예시)
  var currentNode: Node = _
}

```
## Merge
수신한 데이터와 기존 남은 데이터를 병합한다.

#### pseudo code
```python
# 데이터 병합 메서드
merge_data():
    for data_list in self.receive_buffers.values():
        self.local_data.extend(data_list)
    # 최종 정렬 수행
    self.local_data = merge_sort(self.local_data)
```

pseudo code first draft from chatgpt