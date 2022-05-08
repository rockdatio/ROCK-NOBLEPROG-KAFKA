package KafkaAdminClient

import java.util.Properties

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}

class KafkaPropertiesClient {
  def printIntroduction(): Unit = {
    println("**" * 52)
    println("**" * 20)
    println("Menú de opciones : ")
    println("**" * 20)
    println("VM arguemtns :")
    println("-DinputTopic=mx-transaction -Dbrokers=localhost:9092 -Doption=1 -Dpartitions=1 -DreplicationFactor=1")
    println("**" * 20)
    println("case 1 => getClusterDescription()")
    println("case 2 => getEachTopicName(names)")
    println("case 3 => createTopic(topicName = inputTopic,partitions = partitions.toInt,replicationFactor = replicationFactor.toInt)")
    println("case 4 => describeTopic(topicName = inputTopic)")
    println("case 5 => getAllConsumerGroup()")
    println("case 6 => getcurrentPartitionReassignments() //List all of the current partition reassignments")
    println("case 7 => deleteTopic(topicName = inputTopic) //delete topic")
    println("case _ => println(\"There is no option chosen.\") // this is a WILDCARD which matches for all other patterns")
    println("**" * 52)
  }

  def getKafkaSesion(brokers: String): AdminClient = {
    val props: Properties = {
      val p = new Properties()
      p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      p
    }
    AdminClient.create(props)
  }
}
