package KafkaBeforeKstreams

import java.util.Properties

import KafkaProducersClient.ProducerClient.brokers
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

class PropertiesApi(brokers: String) {

  def propertiesConsumer(autoOffsetReset: String, groupId: String): Properties = {
    val propsConsumer: Properties = new Properties()
    propsConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    propsConsumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset)
    propsConsumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    propsConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer") //simpleconsumer
    //    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer")//kafkastreams
    //    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer")//kafkastreams
    propsConsumer
  }

  def propertiesProducer(): Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    //    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getCanonicalName)
    //    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getCanonicalName)
    props
  }
}
