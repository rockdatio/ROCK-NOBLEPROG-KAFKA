package KafkaAdminClient

import java.util
import java.util.concurrent.{ExecutionException, TimeUnit, TimeoutException}
import java.util.{Collections, Map}

import org.apache.kafka.clients.admin._
import org.apache.kafka.common.{KafkaFuture, Node, TopicPartition, Uuid}

object KafkaTopicsAdmin extends KafkaPropertiesClient {
  printIntroduction()
  private val brokers = sys.props.get("brokers").get
  println("brokers : " + brokers)
  private val inputTopic = sys.props.get("inputTopic").get
  println("inputTopic : " + inputTopic)
  private val option = sys.props.get("option").get
  println("option : " + option)
  private val partitions = sys.props.get("partitions").get
  println("partitions : " + partitions)
  private val replicationFactor = sys.props.get("replicationFactor").get
  println("replicationFactor : " + replicationFactor)

  private val adminClient: AdminClient = getKafkaSesion(brokers)

  def main(args: Array[String]): Unit = {
    val names: KafkaFuture[util.Set[String]] = adminClient.listTopics().names()
    try {
      option.toInt match {
        case 1 => getClusterDescription()
        case 2 => getEachTopicName(names)
        case 3 => createTopic(
          topicName = inputTopic,
          partitions = partitions.toInt,
          replicationFactor = replicationFactor.toInt)
        case 4 => describeTopic(topicName = inputTopic)
        case 5 => getAllConsumerGroup()
        case 6 => getcurrentPartitionReassignments() //List all of the current partition reassignments
        case 7 => deleteTopic(topicName = inputTopic) //List all of the current partition reassignments
        case _ => println("There is no option chosen.") // this is a WILDCARD which matches for all other patterns
      }
    } catch {
      case e: InterruptedException => println("Exception occured during retrieval."+ e.toString)
      case e: ExecutionException => println("Exception occured during retrieval." + e.toString)
      case e: TimeoutException => println("Time out exception, while trying to connect."+ e.toString)
      case _: Throwable => println("Exception occured during retrieval.")
    }
    finally {
      adminClient.close()
    }
  }

  private def getClusterDescription(): Unit = {
    val cluster = adminClient.describeCluster()
    val clusterId: KafkaFuture[String] = cluster.clusterId()
    val nodes: KafkaFuture[util.Collection[Node]] = cluster.nodes()
    KafkaFuture.allOf(clusterId).get(10.toLong, TimeUnit.SECONDS)
    val id: String = clusterId.get()
    println("**************")
    println("Got clusterID:")
    println("**************")
    println(id)
    KafkaFuture.allOf(nodes).get(10.toLong, TimeUnit.SECONDS)
    println("**************")
    println("Getting Cluster Nodes description:")
    println("**************")
    val nodesList = nodes.get()
    val nodesListIterator = nodesList.iterator()
    while (nodesListIterator.hasNext()) {
      println("-------------------")
      println(nodesListIterator.next())
    }

  }

  private def getEachTopicName(names: KafkaFuture[util.Set[String]]): Unit = {
    KafkaFuture.allOf(names).get(10.toLong, TimeUnit.SECONDS)
    val topics = names.get()
    val topic_itr: util.Iterator[String] = topics.iterator()
    println("***********")
    println("Got Created topics:")
    println("***********")
    while (topic_itr.hasNext()) {
      println("-------------------")
      println(topic_itr.next())
    }
  }

  def createTopic(topicName: String, partitions: Int, replicationFactor: Int): Unit = {
    val configs = collection.mutable.Map(
      "retention.bytes" -> "-1",
      "retention.ms" -> "60000", // 60000 1 minuto // 604800000 (7 days)
    )

    val newTopic: KafkaFuture[Uuid] = adminClient
      .createTopics(
        Collections.singletonList( // * Returns an immutable list containing only the specified object.
          new NewTopic(
            topicName,
            partitions,
            replicationFactor.toShort
          )
            .configs(scala.collection.JavaConverters.mapAsJavaMap(configs))
        )
      ).topicId(topicName)

    KafkaFuture.allOf(newTopic).get(10.toLong, TimeUnit.SECONDS)
    println("***************")
    println("New topic Created:")
    println("***************")
    println(newTopic.get())
  }

  def describeTopic(topicName: String): Unit = {
    val v: util.Vector[String] = new java.util.Vector()
    v.add(topicName)

    val topicNameEnumm: util.Enumeration[String] = v.elements()
    val listTopics: util.ArrayList[String] = Collections.list(topicNameEnumm);
    val describedTopics: KafkaFuture[util.Map[String, TopicDescription]] = adminClient.describeTopics(listTopics).all()

    KafkaFuture.allOf(describedTopics).get(10.toLong, TimeUnit.SECONDS)
    val descriptions: Map[String, TopicDescription] = describedTopics.get()
    val iteratorDescripciton: util.Iterator[Map.Entry[String, TopicDescription]] = descriptions.entrySet().iterator()
    println("************************")
    println("Topics descriptions:")
    println("************************")
    while (iteratorDescripciton.hasNext()) {
      val entry: Map.Entry[String, TopicDescription] = iteratorDescripciton.next()
      println(entry.getKey() + ":" + entry.getValue())
    }
  }

  def getAllConsumerGroup(): Unit = {
    val names: KafkaFuture[util.Collection[ConsumerGroupListing]] = adminClient.listConsumerGroups().all()
    KafkaFuture.allOf(names).get(10.toLong, TimeUnit.SECONDS)
    val consumerGroup: util.Collection[ConsumerGroupListing] = names.get()
    val consumerGroupIterator: util.Iterator[ConsumerGroupListing] = consumerGroup.iterator()
    println("***********")
    println("Got Created Consumer-groups:")
    println("***********")
    while (consumerGroupIterator.hasNext()) {
      println("-------------------")
      println(consumerGroupIterator.next())
    }
  }

  def getcurrentPartitionReassignments(): Unit = {
    val reassignments: KafkaFuture[util.Map[TopicPartition, PartitionReassignment]] = adminClient.listPartitionReassignments().reassignments()
    KafkaFuture.allOf(reassignments).get(10.toLong, TimeUnit.SECONDS)
    val reassignmentsInfo: util.Map[TopicPartition, PartitionReassignment] = reassignments.get()
    val reassignmentsInfoIterator = reassignmentsInfo.entrySet().iterator()
    println("***********")
    println("Got reassignments Information:")
    println("***********")
    while (reassignmentsInfoIterator.hasNext()) {
      println("-------------------")
      val result = reassignmentsInfoIterator.next()
      println(result.getKey, result.getValue)
    }
  }

  def deleteTopic(topicName: String): Unit = {
    val v: util.Vector[String] = new java.util.Vector()
    v.add(topicName)
    val topicNameEnumm: util.Enumeration[String] = v.elements()
    val listTopics: util.ArrayList[String] = Collections.list(topicNameEnumm);
    val newTopic = adminClient.deleteTopics(listTopics)
    KafkaFuture.allOf(newTopic.all()).get(10.toLong, TimeUnit.SECONDS)
    println("***************")
    println("New topic Created:")
    println("***************")
    println(newTopic.values().get())

  }
}
