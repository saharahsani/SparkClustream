package com.backhoff.clustream

import breeze.linalg.DenseVector
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaStreamingTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark CluStream").setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "2000")
      .set("spark.ui.enabled", "True")
      .set("spark.ui.port", "4040")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val ssc = new StreamingContext(sc, Milliseconds(1000))
    // val lines = ssc.socketTextStream("localhost", 9998)

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

    val model = new CluStreamOnline(50, 54, 2000).removeExpiredSW(true).setDelta(512).setM(20).setInitNormalKMeans(false)
    val clustream = new CluStream(model)
    ssc.addStreamingListener(new PrintClustersListener(clustream, sc))
   // if(!Setting.initialize) {
    //  val bool = clustream.StartInitialize(Setting.snapsPath,sc: SparkContext, Setting.initPathFile)
    //  if (bool) Setting.initialize = true
  //  }
   // if(Setting.initialize) {
        clustream.startOnline(stream.map(z => z._2.split(",").map(_.toDouble)).map(DenseVector(_)))
   // }
    ssc.start()
    ssc.awaitTermination()
  }

}



private[clustream] class PrintClustersListener(clustream: CluStream, sc: SparkContext) extends StreamingListener {

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    if (batchCompleted.batchInfo.numRecords > 0) {

      val tc = clustream.model.getCurrentTime
      val n = clustream.model.getTotalPoints
      clustream.saveSnapShotsToDisk(Setting.snapsPath, tc, 2, 10)
      println("tc = " + tc + ", n = " + n)
      // expiring phase
      if(Setting.expirePhase) {
        val res = clustream.expiringPhase(tc, Setting.windowTime, Setting.snapsPath)
      }
      if (tc >=Setting.centersStartNum) {
       // online phase centers
        OnlineCenters.getCenters(clustream, Setting.snapsPath, tc)

        // offline phase centers
      // OfflineCenters.getOfflineCenters(sc,clustream,Setting.snapsPath,tc)
      }

      //      if (149900 < n && n <= 150100 ) {
      //
      //        val snaps = clustream.getSnapShots("snaps",tc,256)
      //        val clusters = clustream.fakeKMeans(sc, 5, 2000, clustream.getMCsFromSnapshots("snaps", tc, 256))
      //        println("=============  MacroClusters Centers for time = " + tc + ", n = " + n + ", snapshots = " + snaps + " ============")
      //        clusters.clusterCenters.foreach(c=>scala.tools.nsc.io.Path("/home/omar/datasets/tests/2case/results/clustream200/centers6").createFile().appendAll(c.toArray.mkString("",",","") +"\n" ))
      //
      //
      ////        val clusters = clustream.fakeKMeans(sc, 5, 2000, clustream.model.getMicroClusters)
      ////        println("=============  MacroClusters Centers for time = " + tc + ", n = " + n + " ============")
      ////        clusters.clusterCenters.foreach(println)
      //
      //      }
      //      if( 249900 < n && n <= 250100){
      //        val snaps = clustream.getSnapShots("snaps",tc,256)
      //        val clusters = clustream.fakeKMeans(sc, 5, 2000, clustream.getMCsFromSnapshots("snaps", tc, 256))
      //        println("=============  MacroClusters Centers for time = " + tc + ", n = " + n + ", snapshots = " + snaps + " ============")
      //        clusters.clusterCenters.foreach(c=>scala.tools.nsc.io.Path("/home/omar/datasets/tests/2case/results/clustream200/centers21").createFile().appendAll(c.toArray.mkString("",",","")+"\n"))
      //      }
      //      if(349900 < n && n <= 350100 ){
      //        val snaps = clustream.getSnapShots("snaps",tc,256)
      //        val clusters = clustream.fakeKMeans(sc, 5, 2000, clustream.getMCsFromSnapshots("snaps", tc, 256))
      //        println("=============  MacroClusters Centers for time = " + tc + ", n = " + n + ", snapshots = " + snaps + " ============")
      //        clusters.clusterCenters.foreach(c=>scala.tools.nsc.io.Path("/home/omar/datasets/tests/2case/results/clustream200/centers81").createFile().appendAll(c.toArray.mkString("",",","")+"\n"))
      //      }
      //      if(449900 < n && n <= 450100){
      //        val snaps = clustream.getSnapShots("snaps",tc,256)
      //        val clusters = clustream.fakeKMeans(sc, 5, 2000, clustream.getMCsFromSnapshots("snaps", tc, 256))
      //        println("=============  MacroClusters Centers for time = " + tc + ", n = " + n + ", snapshots = " + snaps + " ============")
      //        clusters.clusterCenters.foreach(c=>scala.tools.nsc.io.Path("/home/omar/datasets/tests/2case/results/clustream200/centers161").createFile().appendAll(c.toArray.mkString("",",","")+"\n"))
      //      }

    }
  }
}


