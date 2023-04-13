package KafkaProducersClient

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.StdIn

object ProducerClient extends App {
  println("***** ProducerClient VM arguemtns : ")
  println("-Dbrokers=kafka1:19092,kafka2:19093,kafka3:19094 -DinputTopic=mx-transaction-input")
  private val brokers = sys.props.get("brokers").get
  println("brokers : " + brokers)
  private val inputTopic = sys.props.get("inputTopic").get
  println("inputTopic : " + inputTopic)

  private def configuration: Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    //    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getCanonicalName)
    //    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getCanonicalName)
//    props.put("kafkastore.security.protocol", "ssl")
//    props.put("kafkastore.ssl.truststore.location", "ssl_truststore_location")
//    props.put("kafkastore.ssl.truststore.password", "ssl_truststore_password")
//    props.put("kafkastore.ssl.keystore.location", "ssl_keystore_location")
//    props.put("kafkastore.ssl.keystore.password", "ssl_keystore_password")
//    props.put("kafkastore.ssl.key.password", "ssl_keystore_password")
    props
  }

  val producer = new KafkaProducer[String, String](configuration)

  println("Enter message (type exit to quit)")
  var message = StdIn.readLine()
  while (!message.equals("exit")) {
    message = StdIn.readLine()
//    val record = new ProducerRecord[String, String](inputTopic, "1", message)
    val record = new ProducerRecord[String, String](inputTopic, message)
    producer.send(record)
  }
  producer.close()
}
