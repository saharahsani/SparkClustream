package com.backhoff.clustream

import breeze.linalg.DenseVector
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.time.{Duration, Instant}

object KafkaStreamingTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark CluStream").setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "1000")
      .set("spark.ui.enabled", "True")
      .set("spark.ui.port", "4040")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val ssc = new StreamingContext(sc, Milliseconds(1000))
    // val lines = ssc.socketTextStream("localhost", 9998)
    val log = LogManager.getRootLogger
   // log.setLevel(Level.INFO)
    val topic = "cluTest7"
    val m = Map(
      "bootstrap.servers" -> "localhost:9092",
      "group.id" -> "test",
      "auto.offset.reset" -> "largest",
      "metadata.broker.list" -> "localhost:9092"
      /* "max.poll.interval.ms" ->"1000",
       "max.poll.records" ->"2000"*/
    )
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, m, Set(topic))

    val model = new CluStreamOnline(100, Setting.numDimension, 2000).removeExpiredSW(Setting.expirePhase).setDelta(512).setM(20).setInitNormalKMeans(true)
    val clustream = new CluStream(model)
    ssc.addStreamingListener(new PrintClustersListener(clustream, sc))
   // if(!Setting.initialize) {
    //  val bool = clustream.StartInitialize(Setting.snapsPath,sc: SparkContext, Setting.initPathFile)
    //  if (bool) Setting.initialize = true
  //  }
   // if(Setting.initialize) {
    clustream.startOnline(stream.map(z => z._2.split(",").map(_.toDouble)).map(DenseVector(_)))
    //clustream.startOnline(stream.map(z => Vectors.dense(z._2.split(",").map(_.toDouble))))//.map(DenseVector(_)))

    // }
    ssc.start()
    ssc.awaitTermination()
  }

}



private[clustream] class PrintClustersListener(clustream: CluStream, sc: SparkContext) extends StreamingListener {
  var durationStep:Long=0L

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    if (batchCompleted.batchInfo.numRecords > 0) {

      val tc = clustream.model.getCurrentTime
      val n = clustream.model.getTotalPoints

      clustream.saveSnapShotsToDisk(Setting.snapsPath, tc, 2, 10)


      println("tc = " + tc + ", n = " + n)
      // expiring phase
      /*     if(Setting.expirePhase) {
        val res = clustream.expiringPhase(tc, Setting.windowTime, Setting.snapsPath)
      }*/
      if (tc >=Setting.centersStartNum) {
       // online phase centers
        OnlineCenters.getCenters(clustream, Setting.snapsPath, tc)
      //  OnlineCenters.getCenters(clustream, Setting.snapsPath, tc,Setting.h)

        // offline phase centers
      // OfflineCenters.getOfflineCenters(sc,clustream,Setting.snapsPath,tc)
      }

    /*  if (tc == 5 | tc == 10 | tc == 20 | tc == 40 | tc == 80 | tc == 120 |  tc == 100 |tc == 160 | tc == 195) {
        //
        // val snaps = clustream.getSnapShots("src/test/resources/snaps",tc,1)
        val clusters = clustream.fakeKMeans(sc, 10, 5000, clustream.getMCsFromSnapshots("src/test/resources/snaps", tc, 1))
        //println("=============  MacroClusters Centers for time = " + tc + ", n = " + n + ", snapshots = " + snaps + " ============")
        clusters.clusterCenters.foreach(c => scala.tools.nsc.io.Path("src/test/resources/result/teeeest/wew").createFile().appendAll(c.toArray.mkString("", ",", "") + "\n"))
      }
      val t1 = Instant.now()
      durationStep += Duration.between(t0, t1).toMillis
      if (tc == 5 | tc == 10 | tc == 20 | tc == 40 | tc == 80 | tc == 120 | tc == 100 | tc == 160 | tc == 195) {
        println(s"execution saveSnp & offline time${tc}: ${durationStep} ms")
      }*/
    }
  }
}


