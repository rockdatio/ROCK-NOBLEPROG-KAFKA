package KafkaSingleConsumerClient

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

object ConsumerClient {
  println("Vm arguments :")
  println("-DinputTopic=mx-transaction -Dbrokers=0.0.0.0:9091,0.0.0.0:9092,0.0.0.0:9093 -DgroupId=test -DautoOffsetReset=latest")
  private val brokers = sys.props.get("brokers").get
  println("brokers : " + brokers)
  private val inputTopic = sys.props.get("inputTopic").get
  println("inputTopic : " + inputTopic)
  private val groupId = sys.props.get("groupId").get
  println("groupId : " + groupId)
  private val autoOffsetReset = sys.props.get("autoOffsetReset").get
  println("autoOffsetReset : " + autoOffsetReset)

  def main(args: Array[String]): Unit = {
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

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(inputTopic))

    println("Recieving messages")
    println("Sesion Iniciada")
    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(1000)
      records.asScala.foreach(record => {
        println(s"Received message: ${record.value()}")
        println(s"headers: ${record.headers()}")
        println(s"key: ${record.key()}")
        println(s"offset: ${record.offset()}")
        println(s"partition: ${record.partition()}")
        println(s"serializedKeySize: ${record.serializedKeySize()}") // The size of the serialized, uncompressed key in bytes. If key is null, the returned size
        println(s"serializedValueSize: ${record.serializedValueSize()}")
        println(s"timestamp: ${record.timestamp()}")
        println(s"timestampType: ${record.timestampType()}")
        println(s"Topic: ${record.topic()}")
      }
      )
    }
  }
}