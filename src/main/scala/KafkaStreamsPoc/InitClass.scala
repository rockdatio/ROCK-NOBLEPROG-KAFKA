package KafkaStreamsPoc

class InitClass extends Runnable{
  val brokers = sys.props.get("brokers").get
  println("brokers : " + brokers)
  val inputTopic = sys.props.get("inputTopic").get
  println("inputTopic : " + inputTopic)
  val outputTopic = sys.props.get("outputTopic").get
  println("outputTopic : " + outputTopic)
  val groupId = sys.props.get("groupId").get
  println("groupId : " + groupId)
  val autoOffsetReset = sys.props.get("autoOffsetReset").get
  println("autoOffsetReset : " + autoOffsetReset)
  override def run(): Unit = {
    println("VM arguemtns :")
    println("-DinputTopic=inputTopic -DoutputTopic=outputTopic -Dbrokers=kafka1:19092,kafka2:19093,kafka3:19094 -DgroupId=test -DautoOffsetReset=latest")
  }
}
