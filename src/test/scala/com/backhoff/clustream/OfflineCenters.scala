package com.backhoff.clustream


import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import java.nio.file.{Files, Paths}
import java.time.{Duration, Instant}
import scala.reflect.io.Path

object OfflineCenters {
  def main(args: Array[String]): Unit = {
    var startNum =4
    val loopEndNum = 200
    val dir = Setting.snapsPath
    val h = Setting.windowTime
    val k =10
    val numPoint = 5000

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    sc.setLogLevel("ERROR")
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)

    val dirPath = Paths.get(s"${Setting.centersOfflinePath}")
    val directory = new File(dirPath.toString)
    if (!directory.exists()) {
      Files.createDirectories(dirPath)
    }
    val clustream = new CluStream(null)
    var durationStep: Long = 0L

    for (tc <- startNum to loopEndNum) {

      val snap1 = clustream.getMCsFromSnapshots(dir, tc, h.toLong)
      println("snapshots " + clustream.getSnapShots(dir, tc, h.toLong))
      println("mcs count: "+snap1.filter(x=>x.getN!=0).length)
      println(snap1.map(a => a.getN).mkString("[", ",", "]"))
      println("mics points = " + snap1.map(_.getN).sum)
      val t0=Instant.now
      val clusters = clustream.fakeKMeans(sc, k, numPoint, snap1.filter(x=>x.getN!=0))

      val t1=Instant.now
      if(tc==5 | tc==10 | tc==20 | tc==40 | tc==80 | tc==100| tc==150 | tc==200) {
        durationStep = Duration.between(t0, t1).toMillis
        println(s"-------------  execution time${tc}: ${durationStep} ms  ---------------")
      }
      if (clusters != null) {

        clusters.clusterCenters.foreach(c => Path(s"${Setting.centersOfflinePath}/centers${tc}").createFile().appendAll(c.toArray.mkString("", ",", "") + s"_${Setting.runNum}" + "\n"))
      }

    }

  }


  def getOfflineCenters(sc:SparkContext,clustream:CluStream,snaps:String,tc:Long) {
    val snapshot = clustream.getMCsFromSnapshots(snaps, tc,70)
    //println("snapshots----> " + tc) //clustream.getSnapShots(snaps, number, 70)
    //println(snapshot.map(a => a.getN).mkString("[", ",", "]"))
   // println("mics points = " + snapshot.map(_.getN).sum)
    val t0=Instant.now
    val clusters = clustream.fakeKMeans(sc, 5, 5000, snapshot)

    val t1=Instant.now

    clusters.clusterCenters.foreach(c => scala.tools.nsc.io.Path(s"src/test/resources/offlineKdd2_sw/centers${tc}").createFile().appendAll(c.toArray.mkString("", ",", "") + "\n"))

  }
}