package KafkaStreamsPoc

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object ConsumerKstreams {
  println("VM arguemtns :")
  println("-DinputTopic=inputTopic -DoutputTopic=outputTopic -Dbrokers=kafka1:19092,kafka2:19093,kafka3:19094 -DgroupId=test -DautoOffsetReset=latest")

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
    val props: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "KafkaStreamApplicationWordCount")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      p.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT")
      p
    }

    val builder: StreamsBuilder = new StreamsBuilder
    val textLines: KStream[String, String] = builder.stream[String, String](inputTopic)
    val wordCounts: KStream[String, String] = textLines
      .flatMapValues(textLine => {
        println("Receiving messages ...")
        textLine.toLowerCase.split("\\W+")
      })

    val wordCounts2: KTable[String, Long] = wordCounts
      .groupBy((_, word) => word)
      .count()(Materialized.as("counts-store"))

    wordCounts2.toStream.to(outputTopic)

    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()
    println(streams.toString)
    sys.ShutdownHookThread {
      streams.close(10, TimeUnit.SECONDS)
    }
  }
}