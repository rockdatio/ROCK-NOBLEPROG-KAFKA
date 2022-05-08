package KafkaGroupConsumerClient

import java.util.concurrent.Executors

import org.apache.kafka.clients.consumer.KafkaConsumer

class MultipleConsumerClient(
                              brokers: String,
                              consumer: KafkaConsumer[String, String],
                              partitions: String
                            ) extends Runnable {
  override def run(): Unit = {
    val executorService = Executors.newFixedThreadPool(partitions.toInt)
    println(brokers)
    for (a <- 1 to partitions.toInt) {
      println("Thread number : " + a.toString )
      executorService.submit(new ConsumerClientRunner(consumer))
    }
  }
}
