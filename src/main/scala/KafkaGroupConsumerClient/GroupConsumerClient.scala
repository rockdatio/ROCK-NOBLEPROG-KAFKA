package KafkaGroupConsumerClient

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.log4j.Logger

object GroupConsumerClient extends App {
  println("MultipleConsumerClient VM arguemtns :")
  println("-DinputTopic=inputTopic -Dthreads=4 -Dbrokers=0.0.0.0:9091,0.0.0.0:9092,0.0.0.0:9093 -DgroupId=test -DautoOffsetReset=latest")
  private val threads = sys.props.get("threads").get
  println("threads : " + threads)
  private val brokers = sys.props.get("brokers").get
  println("brokers : " + brokers)
  private val inputTopic = sys.props.get("inputTopic").get
  println("inputTopic : " + inputTopic)
  private val groupId = sys.props.get("groupId").get
  println("groupId : " + groupId)
  private val autoOffsetReset = sys.props.get("autoOffsetReset").get
  println("autoOffsetReset : " + autoOffsetReset)

  //  val logger = Logger.getLogger(this.getClass.getName)
  private def configuration: Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer") //simpleconsumer
    //    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer")//kafkastreams
    //    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer")//kafkastreams
    props
  }

  //  logger.info("Logger : Welcome to log4j")
  val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](configuration)
  consumer.subscribe(util.Arrays.asList(inputTopic))

  val appThread = new Thread(new MultipleConsumerClient(brokers, consumer, threads))
  appThread.start()
}