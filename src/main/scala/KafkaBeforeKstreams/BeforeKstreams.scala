package KafkaBeforeKstreams

import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConverters._

object BeforeKstreams {
  println("**** BeforeKstreams Vm arguments :")
  println("-DinputTopic=mx-transaction -DoutputTopic=mx-transaction-output -Dbrokers=kafka1:19092,kafka2:19093,kafka3:19094 -DgroupId=test -DautoOffsetReset=latest")
  private val brokers = sys.props.get("brokers").get
  println("brokers : " + brokers)
  private val inputTopic = sys.props.get("inputTopic").get
  println("inputTopic : " + inputTopic)
  private val outputTopic = sys.props.get("outputTopic").get
  println("outputTopic : " + outputTopic)
  private val groupId = sys.props.get("groupId").get
  println("groupId : " + groupId)
  private val autoOffsetReset = sys.props.get("autoOffsetReset").get
  println("autoOffsetReset : " + autoOffsetReset)

  def main(args: Array[String]): Unit = {
    val properties = new PropertiesApi(brokers)
    val consumer = new KafkaConsumer[String, String](properties.propertiesConsumer(autoOffsetReset, groupId))
    consumer.subscribe(util.Arrays.asList(inputTopic))
    val producer = new KafkaProducer[String, String](properties.propertiesProducer())

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

        val producerRecord: ProducerRecord[String, String] = new ProducerRecord[String, String](outputTopic, record.value())
        producer.send(producerRecord)
      }
      )
    }
  }
}