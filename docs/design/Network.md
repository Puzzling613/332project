# Use gRPC with protobuf(Recommended)
* How to References: https://grpc.io/docs/what-is-grpc/introduction/
https://coding-start.tistory.com/353
* protobuf: pb compiler creates a case class that implements automatic encoding and parsing of the protocol buffer data with an efficient binary format. Importantly, the protocol buffer format supports the idea of extending the format over time in such a way that the code can still read data encoded with the old format
* https://github.com/scalapb/ScalaPB/tree/master/examples/basic
* https://protobuf.dev/

> message type을 .proto file에 정의  
> service 내에 worker,master 정의해서 작업 전달 및 보고 진행.

* IP/port 전달(w->m)->sampling 요청(m->w)->sample data stream 전달 (w->m)  
* 처리 및 결과 반환(m->w)->partitioning (m->w)->sorting 요청(m->w)->worker간 shuffling 요청 (m->w)->merging 요청 (m->w)  
* +)status report(수시, w->m)

1. Init
* Request (Worker -> Master): InitRequest
* Response (Master -> Worker): InitResponse
* Protocol Buffers:
```scala
message InitRequest {
  string ip = 1;
  int32 port = 2;
}
message InitResponse {
  bool isValid = 1;
  string message = 2;
}
```

2. Parsing
* Request (Master -> Worker): ParsingRequest
* Master가 특정 데이터 블록 X의 처리를 요청.
* Response (Worker -> Master): ParsingResponse
* Worker가 해당 데이터 블록의 파싱 완료 상태를 반환.
```scala
message ParsingRequest {
  string blockId = 1; // Identifier for the data block to be parsed
}
message ParsingResponse {
  bool isParsingComplete = 1;
  string message = 2;
}
```

3. Sampling
* Request (Worker -> Master): SamplingRequest
* Worker가 샘플 데이터를 Master에 전송. sample key value list 전달.
* Response (Master -> Worker): SamplingResponse
* Master가 샘플 데이터 처리 상태를 반환.
```scala
message SamplingRequest {
  repeated KeyValueRecord samples = 1;
}
message SamplingResponse {
  bool isSamplingAccepted = 1;
  string message = 2;
}
```

4. Partitioning
* Request (Master -> Worker): PartitioningRequest
* Master가 데이터 블록의 분할 boundary를 Worker에게 전달.
* Response (Worker -> Master): PartitioningResponse
* Worker가 작업 완료 상태를 Master에 보고.
```scala
message PartitioningRequest {
  repeated KeyValueRecord partitions = 1; 
}
message PartitioningResponse {
  bool isPartitioningComplete = 1;
  string message = 2;
}
```

5. Sorting
* Request (Master -> Worker): SortingRequest
* Master가 정렬을 요청.
* Response (Worker -> Master): SortingResponse
* Worker가 정렬 완료 상태와 정렬된 데이터 정보를 반환.
```scala
message SortingRequest {
  string blockId = 1; //which block to sort
}
message SortingResponse {
  bool isSortingComplete = 1; // true if sorting is complete
  string message = 2;
  repeated KeyValueRecord sortedData = 3;
}
```

6. Shuffle
* Request (Master -> Worker): ShuffleRequest
* Master가 shuffling 요청.
* Response (Worker -> Master): ShuffleResponse
* Worker가 교환된 데이터 블록 상태를 반환.
```scala
message ShuffleRequest {
  string sourceWorkerId = 1; // ID of the src worker
  string targetWorkerId = 2; // ID of the target worker
  repeated KeyValueRecord shuffledData = 3;ㄴ
}
message ShuffleResponse {
  bool isShuffleComplete = 1;
  string message = 2;
}
```

7. Merging
* Request (Master -> Worker): MergeRequest
* Master가 데이터 병합 요청. list of data stream 전달.
* Response (Worker -> Master): MergeResponse
* Worker가 병합된 데이터 상태를 반환.
```scala
message MergeRequest {
  repeated string blockIds = 1;
}
message MergeResponse {
  bool isMergeComplete = 1;
  string message = 2;
}
```

8. Status Reporting
* Request (Worker -> Master): StatusReport
* Worker가 현재 상태(작업 진행률, 오류 등)를 Master에 보고.
* Response (Master -> Worker): StatusAcknowledgment
* Master가 상태 보고에 대한 확인을 반환.
```scala
message StatusReport {
  int32 workerId = 1;
  string status = 2;
  string errorDetails = 3;
}
message StatusAcknowledgment {
  bool isAcknowledged = 1;
  string message = 2;
}
```

* Async Stub: callback 기반.  
e.g. 

```scala
import io.grpc.stub.StreamObserver;

public void initializeWorker(String ip, int port) {
    InitRequest request = InitRequest.newBuilder()
            .setIp(ip)
            .setPort(port)
            .build();

    asyncStub.init(request, new StreamObserver<InitResponse>() {
        @Override
        public void onNext(InitResponse response) {
            if (response.getIsValid()) {
                System.out.println("Worker initialized successfully!");
            } else {
                System.out.println("Worker initialization failed: " + response.getMessage());
            }
        }

        @Override
        public void onError(Throwable t) {
System.err.println("Initialization failed: " + t.getMessage());
        }

        @Override
        public void onCompleted() {
System.out.println("Initialization request completed.");
        }
    });
}
```
