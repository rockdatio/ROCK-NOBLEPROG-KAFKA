package KafkaProducersClient

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.time.{LocalDateTime, LocalTime, ZoneId}
import KafkaProducersClient.ProducerPowerBi.sleep
import Utils.BaseFunctions
import com.google.gson.JsonObject
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

object OnlyTestProducerClient {
  def main(args: Array[String]): Unit = {
    println("***** ProducerClient VM arguemtns : ")
    println("-Dbrokers=kafka1:19092,kafka2:19093,kafka3:19094 -DinputTopic=mx-transaction-input")
    val brokers = sys.props.get("brokers").get
    println("brokers : " + brokers)
    val inputTopic = sys.props.get("inputTopic").get
    println("inputTopic : " + inputTopic)
    //    val broadPath = sys.props.get("broadPath").get
    //    println("broadPath : " + broadPath)

    def configuration: Properties = {
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
      //    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getCanonicalName)
      //    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getCanonicalName)
      props
    }

    val producer = new KafkaProducer[String, String](configuration)

    val reader = new ReaderSource
    //  val broadList: Array[String] = reader.readBroad("C:\\workspace\\workspace\\ROCK-NOBLEPROG-KAFKA\\src\\main\\resources\\data-streams.txt")
    //    val broadList: Array[String] = reader.readBroad(broadPath)
    val broadList: Array[String] = reader.readBroad("C:\\workspace\\workspace\\ROCK-NOBLEPROG-KAFKA\\src\\main\\resources\\data-streams.txt")
    var limit = 0
    val until = 10000
    broadList.foreach(println(_))

    while (limit < until) {
      broadList.foreach {
        message => {
          limit += 1
//          Thread.sleep(2000)
          val jsonRecord: JsonObject = BaseFunctions.getJson(message)
          jsonRecord.addProperty("transaction_date",
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now))
          jsonRecord.remove("amount")
          jsonRecord.addProperty("amount", BaseFunctions.getAmountRandom(100, 5000))
          jsonRecord.addProperty("headerId", "X-" + limit)
          println(jsonRecord)
          println(limit)
          val record: ProducerRecord[String, String] = new ProducerRecord[String, String](inputTopic, jsonRecord.toString)
          producer.send(record, new Callback {
            override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
              if (e != null) {
                println(s"Cannot publish to $inputTopic. Caused by: ${e.getMessage}", e)
              }
              else {
                val offset = recordMetadata.offset()
                val partition = recordMetadata.partition()
                val key = recordMetadata.topic()
                println(s"record has already sent to kafka:topic=$inputTopic, partition=$partition, offset=$offset");
              }
            }
          })

          // producer.send(record).get() // Blocking code, stop asynchronous request
        }
      }
    }
    producer.close()
  }
}

