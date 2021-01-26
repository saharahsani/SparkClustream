
package com.backhoff.clustream

/*
* Create by Sahar on 7/22/2020 4:04 AM.
*/

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.io.Source

object ProducerTest extends  App() {
  var properties = new Properties()

  val topicName = "cluTest7"

  //Assign localhost id
  properties.put("bootstrap.servers", "localhost:9092")

  //Set acknowledgements for producer requests.
  properties.put("acks", "all")

  //If the request fails, the producer can automatically retry,
  properties.put("retries", "0")

  //Specify buffer size in config
  properties.put("batch.size", "16384")

  //Reduce the no of requests less than 0
  properties.put("linger.ms", "1")

  //The buffer.memory controls the total amount of memory available to the producer for buffering.
  properties.put("buffer.memory", "65536")

  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


  var kafkaProducer = new KafkaProducer[String, String](properties)
  val fileName0 = "src/test/resources/ScaledKdd.txt"
  val fileName = "src/test/resources/kddCup.txt"
  val fileName1 = "src/test/resources/kdd_MinMax_OnlyNumeric.txt"
  val fileName2 = "src/test/resources/minMaxExport.txt"
  val fileName3 = "src/test/resources/zScore_normalize.txt"
  val fileName4="src/test/resources/powersupply.csv"
  val fileName5="src/test/resources/ShuffleDataset.csv"
  val fileName6="src/test/resources/scaledCovtype.csv"
  val fileName7="src/test/resources/Covtype.csv"


  try {
    var i = 0
    for (line <- Source.fromFile(fileName7).getLines) {
      {
        i += 1
        val message = line + "\n"
        val producerRecord = new ProducerRecord[String, String](topicName, message)
        kafkaProducer.send(producerRecord)

      }

    }

    kafkaProducer.close()
    println("close my producer.")
  }
  catch {
    case e: Exception => e.printStackTrace()

  }


}

