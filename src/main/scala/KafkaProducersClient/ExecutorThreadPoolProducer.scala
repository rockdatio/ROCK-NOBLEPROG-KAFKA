package KafkaProducersClient

import java.util.concurrent.Executors

import org.apache.kafka.clients.producer.KafkaProducer

class ExecutorThreadPoolProducer(
                        topic: String,
                        brokers: String,
                        producer: KafkaProducer[String, String],
                        partitions: String,
                        limitMessages: String,
                        broadPath: String,
                        sleep: String
                      ) extends Runnable {
  override def run(): Unit = {
    val executorService = Executors.newFixedThreadPool(partitions.toInt)
    for (a <- 1 to partitions.toInt) {
      println("Value of a: " + a)
      println(brokers)
      executorService.submit(new ThreadProducerClient(topic, producer, limitMessages, broadPath, sleep))
    }
  }
}
