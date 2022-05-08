package KafkaProducersClient

import com.mashape.unirest.http.Unirest
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class ThreadProducerClient(topic: String,
                           producer: KafkaProducer[String, String],
                           limitMessages: String,
                           broadPath: String,
                           sleep: String) extends Runnable {

  override def run(): Unit = {
    val reader = new ReaderSource
    val broadList: Array[String] = reader.readBroad(broadPath)
    val a = ""
    var limit = 0
    val until = limitMessages.toInt
    broadList.foreach(println(_))

    while (limit < until) {
      broadList.foreach {
        message => {
          limit += 1
          println(limit)
          Thread.sleep(sleep.toInt)
          println(message)
//          Unirest
//            .post("https://api.powerbi.com/beta/5d93ebcc-f769-4380-8b7e-289fc972da1b/datasets/7265d43e-1d8c-490f-a372-af79a67a0d7e/rows?key=uMMWfIke7bDqFmTqkdkh7qhcCiwybQ1yqlRQkUYidRRGYd2RAjz4CWUHGb%2BOy21QQVYGrA5xog28OV%2BnovWdEg%3D%3D")
//            .header("Content-Type", "application/json")
//            .body(message)
//            .asJsonAsync()
          val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, message)
          producer.send(record)
        }
      }
    }
    println("ending while")
    //    producer.close()
  }
}
//                      .post("https://api.powerbi.com/beta/c4a66c34-2bb7-451f-8be1-b2c26a430158/datasets/d0539339-c3db-4130-bd2d-70280bca4fa6/rows?key=Vf5M26vH55Fo6HXoSbNIz4Fl9HYTuDnOEmfSgRnPYnOWJNuqKUMBHgVH0hE9sn5HPRsUvVyspAkhJCvoMmbYpg%3D%3D")