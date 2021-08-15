package com.backhoff.clustream

import java.io.File
import java.nio.file.{Files, Paths}
import scala.reflect.io.Path

object OnlineCenters {
  def main(args: Array[String]): Unit = {
   /* val snaps = "src/test/resources/snaps"
    val clustream = new CluStream(null)
    val arrNum = Array(185, 195, 205,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239, 240, 241, 242, 243, 244, 245, 246, 247,248)
    for (number <- arrNum) {
      val snapshot = timer {
        clustream.getMCsFromSnapshotSW(snaps, number)
      }
      println(snapshot.map(a => a.getN).mkString("[", ",", "]"))
      println("mics points = " + snapshot.map(_.getN).sum)
      var centers = clustream.getCentersFromMC(snapshot).map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))
      centers.foreach(c => scala.tools.nsc.io.Path(s"src/test/resources/clustream2000/centers${number}").createFile().appendAll(c.toArray.mkString("", ",", "") + "\n"))
    }*/
  }
  def getCenters(clustream:CluStream,snaps:String,tc:Long,h:Int): Unit = {

    val snapshot = clustream.getMCsFromSnapshots(snaps, tc, h)
   // println("mics points = " + snapshot.map(_.getN).mkString(","))
    val dirPath = Paths.get(s"${Setting.centersOnlinePath}")
    val directory = new File(dirPath.toString)
    if (!directory.exists()) {
      Files.createDirectories(dirPath)
    }
    var centers = clustream.getCentersFromMC(snapshot).map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))
    centers.foreach(c => scala.tools.nsc.io.Path(s"${Setting.centersOnlinePath}/centers${tc}").createFile().appendAll(c.toArray.mkString("", ",", "") + s"_${Setting.runNum}" + "\n"))
  }

  def getCenters(clustream: CluStream, snaps: String, tc: Long): Unit = {

    val snapshot = clustream.getMCsFromSnapshotSW(snaps, tc)

    val dirPath = Paths.get(s"${Setting.centersOnlinePath}")
    val directory = new File(dirPath.toString)
    if (!directory.exists()) {
      Files.createDirectories(dirPath)
    }
    var centers = clustream.getCentersFromMC(snapshot).map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))
    centers.foreach(c => Path(s"${Setting.centersOnlinePath}/centers${tc}").createFile().appendAll(c.toArray.mkString("", ",", "") + s"_${Setting.runNum}" + "\n"))

  }
}