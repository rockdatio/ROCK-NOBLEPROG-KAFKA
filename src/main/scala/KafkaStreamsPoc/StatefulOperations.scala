package KafkaStreamsPoc

import java.time.Duration
import java.util.concurrent.TimeUnit

import org.apache.commons.lang3.StringUtils
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{TimeWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, KStream, KTable, Produced}
import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}

object StatefulOperations extends InitClass {
  def main(args: Array[String]): Unit = {
    val properties = new KstreamProperties(brokers)
    val builder: StreamsBuilder = new StreamsBuilder

    implicit val timeWindowedSerde: Serde[Windowed[String]] = Serdes.timeWindowedSerde[String]
    val consumed: Consumed[String, String] = Consumed.`with`[String, String]
    val produced: Produced[String, Double] = Produced.`with`[String, Double]


    val domesticRetailStreamData: KStream[String, String] = builder.stream(inputTopic)(consumed)
    domesticRetailStreamData.peek((key, value) => println("Incoming record - key: {" + key + "} , value: {" + value + "}"))

    val windowSize = Duration.ofSeconds(20)
    val tumblingWindow = TimeWindows.of(windowSize)

    val domesticDepartmentSalesRevenue: KTable[Windowed[String], Double] = domesticRetailStreamData
      .map((_, value) => (
        StringUtils.substring(
          value,
          StringUtils.ordinalIndexOf(
            value,
            ",",
            5
          ) + 1,
          StringUtils.ordinalIndexOf(
            value,
            ",",
            6)),
        StringUtils.substring(
          value,
          StringUtils.ordinalIndexOf(
            value,
            ",",
            3) + 1,
          StringUtils.ordinalIndexOf(
            value,
            ",",
            4)).toDouble
      ))
      .groupByKey(Grouped.`with`[String, Double])
      .windowedBy(tumblingWindow)
      .reduce((v1, v2) => v1 + v2)

    domesticDepartmentSalesRevenue.toStream.selectKey((k, v) => k.key()).to(outputTopic)(produced)
    domesticDepartmentSalesRevenue.toStream.selectKey((k, v) => k.key()).peek((key, value) => println("Outgoing record - key: {" + key + "} , value: {" + value + "}"))

    val topology: Topology = builder.build()
    println(topology.describe())

    val streams: KafkaStreams = new KafkaStreams(
      builder.build(),
      properties.getKstreamProperties("StatefulOperations"))

    streams.start()
    sys.ShutdownHookThread {
      streams.close(100, TimeUnit.SECONDS)
    }
  }
}
