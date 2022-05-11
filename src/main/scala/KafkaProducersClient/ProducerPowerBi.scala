package KafkaProducersClient

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, LocalTime, ZoneId}
import java.util.Properties

import Utils.BaseFunctions
import com.google.gson.JsonObject
import com.mashape.unirest.http.Unirest
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.KafkaFuture.BaseFunction
import org.apache.kafka.common.serialization.StringSerializer

object ProducerPowerBi extends App {
  println("VM arguemtns :")
  println("-Dbrokers=kafka1:19092,kafka2:19093,kafka3:19094 -Dsleep=1 -DlimitMessages=100 -DbroadPath=data-streams.txt -DinputTopic=mx-transaction-input")
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

  private def configuration: Properties = {
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
    val broadList: Array[String] = reader.readBroad(broadPath)
    var limit = 0
    val until = limitMessages.toInt
    broadList.foreach(println(_))

    while (limit < until) {
      broadList.foreach {
        message => {
          limit += 1
          println(limit)
          Thread.sleep(sleep.toInt)
          val jsonRecord = BaseFunctions.getJson(message)
          jsonRecord.addProperty("transaction_date",
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now))
          jsonRecord.remove("amount")
          jsonRecord.addProperty("amount", BaseFunctions.getAmountRandom(100,5000))

          println(jsonRecord)
          Unirest
            .post("https://api.powerbi.com/beta/5d93ebcc-f769-4380-8b7e-289fc972da1b/datasets/173bfc77-2eae-4762-9fa2-30bbdd0057a2/rows?key=mVOpNPkldfd76tIlof0TV0sl7Ec14RNXjl9VfhFLxsmlJVJ3o3VInMfCwEgioq95tI2C%2Fs4JOYQV8wJKBiJMjQ%3D%3D")
            .header("Content-Type", "application/json")
            .body(jsonRecord.toString)
            .asJsonAsync()

          val record: ProducerRecord[String, String] = new ProducerRecord[String, String](inputTopic, message)
          producer.send(record)
        }
      }
    }
    producer.close()
  }