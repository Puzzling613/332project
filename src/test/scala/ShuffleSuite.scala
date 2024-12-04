import java.nio.file.Paths

class SamplePhaseTest extends AnyFunSuite {
  def testShuffle(testInputDir: String, answer: String): Unit = {
    val testOutputDir = "test_output"
    val workerService1 = new WorkerService("", "", testInputDir, testOutputDir)
    workerService1.shufflePhase()
    val sampledData1: Seq[KeyValue] = Source.fromFile(Paths.get(testOutputDir).toFile).getLines().flatMap { line =>
      val key = line.take(10)
      val value = line.drop(10)
      Some(KeyValue(key, value))
    }.toSeq
    assert(sampledData1.size === answer)
  }

  test("shuffle") {
    testShuffle("test_input1/", "3")
    testShuffle("test_input1/", "1")
    testShuffle("test_input1/", "9")
  }

}