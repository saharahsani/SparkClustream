package com.backhoff.clustream

import com.backhoff.clustream.SimpleApp.timer
import org.apache.spark.{SparkConf, SparkContext}

object OfflineCenters {
  def main(args: Array[String]): Unit = {


  }


  def getOfflineCenters(sc:SparkContext,clustream:CluStream,snaps:String,tc:Long) {
    val snapshot = clustream.getMCsFromSnapshots(snaps, tc,70)

    //println("snapshots----> " + tc) //clustream.getSnapShots(snaps, number, 70)
    //println(snapshot.map(a => a.getN).mkString("[", ",", "]"))
   // println("mics points = " + snapshot.map(_.getN).sum)

    val clusters = clustream.fakeKMeans(sc, 5, 5000, snapshot)

    clusters.clusterCenters.foreach(c => scala.tools.nsc.io.Path(s"src/test/resources/offlineKdd2_sw/centers${tc}").createFile().appendAll(c.toArray.mkString("", ",", "") + "\n"))

  }
}