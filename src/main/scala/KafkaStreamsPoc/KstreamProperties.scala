package KafkaStreamsPoc

import java.util.Properties

import org.apache.kafka.streams.StreamsConfig

class KstreamProperties(brokers: String) {
  def getKstreamProperties(kstreamApp: String): Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, kstreamApp)
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    p.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT")
//    p.put(StreamsConfig.OPTIMIZE, "ALL")
    p
  }
}
