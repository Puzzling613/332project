# 목표: 다음 주 화요일까지 구현 완료하기

진행 상황:  
각자 서로의 디자인에 대해 얘기를 나누고 피드백하는 시간을 가짐  

예상되는 난관:
sampling에서 너무 skew되어서  worker에서 넘치면 큰일임 -> stratified sampling 할 것이기도 하고그럴 일은 거의 없을 것 같은데 염두에 둠
동시성 문제 promise and future로 해결하기

홍지우 - sampling/partition 알고리즘 설정 및 의사 코드 구현  
조민석 - shuffle/merge 의사 코드 구현 및 동시성 문제 토의, gensort 이용 구현  
백지은 - grpc 사용방법, worker/master 간 메시지 request/response 언제 해야 하는지 및 각 메시지에 어떤 내용이 들어가야 하는 지 정리, server/client 구현 및 통신  

다음 회의: 화요일 오후 6시 15분
