package KafkaGroupConsumerClient

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import scala.collection.JavaConverters._

class ConsumerClientRunner(consumer: KafkaConsumer[String, String]) extends Runnable {
  override def run(): Unit = {
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