package KafkaProducersClient

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
//import org.apache.log4j.Logger

object MasiveMessageProductorClient extends App {
  println("VM arguemtns :")
  println("-Dbrokers=localhost:9092 -Dsleep=0 -Dthreads=5 -DlimitMessages=100000 -DbroadPath=data-streams.txt -DinputTopic=conf-cloud-com-bcp-shcl-notification-salary-advance")
  private val threads = sys.props.get("threads").get
  println("threads : " + threads + " number of threads")
  private val brokers = sys.props.get("brokers").get
  println("brokers : " + brokers)
  private val inputTopic = sys.props.get("inputTopic").get
  println("inputTopic : " + inputTopic)
  private val limitMessages = sys.props.get("limitMessages").get
  println("limitMessages : " + limitMessages)
  private val broadPath = sys.props.get("broadPath").get
  println("broadPath : " + broadPath)
  private val sleep = sys.props.get("sleep").get
  println("sleep : " + sleep + " expressed in milliseconds")

  //  val logger = Logger.getLogger(this.getClass.getName)
  private def configuration: Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props
  }

  //  logger.info("Logger : Welcome to log4j")
  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](configuration)
  val appThread = new Thread(new ExecutorThreadPoolProducer(inputTopic, brokers, producer, threads, limitMessages, broadPath, sleep))
  appThread.start()
}