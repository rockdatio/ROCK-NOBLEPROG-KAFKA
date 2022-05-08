package KafkaProducersClient
import scala.io.Source

class ReaderSource {
  def readBroad(broadPath: String): Array[String] = {
    val testTxtSource = Source.fromFile(broadPath)
    val broad: Array[String] = testTxtSource.getLines.toArray
    testTxtSource.close()
    broad
  }
}
