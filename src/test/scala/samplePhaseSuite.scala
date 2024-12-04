import java.nio.file.Paths

case class KeyValue(key: String, value: String)

class SamplePhaseTest extends AnyFunSuite {
  def testOnData(testInputDir: String, answer: String): Unit = {
    val testOutputDir = "test_output"
    val workerService1 = new WorkerService("", "", testInputDir, testOutputDir)
    workerService1.samplePhase()
    val sampledData1: Seq[KeyValue] = Source.fromFile(Paths.get(testOutputDir).toFile).getLines().flatMap { line =>
      val key = line.take(10)
      val value = line.drop(10)
      Some(KeyValue(key, value))
    }.toSeq
    println("sampled key from data1:" + sampledData1(0).key)
    assert(sampledData1(0).key === answer)
  }

  test("is it sampled correctly") {
    testOnData("test_input1/", "AsfAGHM5om")
    testOnData("test_input2/", "9SC<z G(1O")
    testOnData("test_input3/", "-^1~=aYZC1")
  }
}
