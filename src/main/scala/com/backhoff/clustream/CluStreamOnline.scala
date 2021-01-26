package com.backhoff.clustream

/**
  * Created by omar on 9/25/15.
  */

import breeze.linalg._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.clustering.{KMeans, StreamingKMeans}
import breeze.stats.distributions.Gaussian

import java.io.{FileInputStream, IOException, ObjectInputStream}
import java.nio.file.{Files, Paths}
import scala.collection.mutable.ArrayBuffer


/**
  * CluStreamOnline is a class that contains all the necessary
  * procedures to initialize and maintain the microclusters
  * required by the CluStream method. This approach is adapted
  * to work with batches of data to match the way Spark Streaming
  * works; meaning that every batch of data is considered to have
  * to have the same time stamp.
  *
  * @param q             : the number of microclusters to use. Normally 10 * k is a good choice,
  *                      where k is the number of macro clusters
  * @param numDimensions : this sets the number of attributes of the data
  * @param minInitPoints : minimum number of points to use for the initialization
  *                      of the microclusters. If set to 0 then initRand is used
  *                      insted of initKmeans
  **/

@Experimental
class CluStreamOnline(
                       val q: Int,
                       val numDimensions: Int,
                       val minInitPoints: Int)
  extends Logging with Serializable {


  /**
    * Easy timer function for blocks
    **/

  def timer[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    logInfo(s"Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  private var mLastPoints = 500
  private var delta = 20
  private var tFactor = 2.0
  private var recursiveOutliersRMSDCheck = true

  private var time: Long = 0L
  private var N: Long = 0L
  private var currentN: Long = 0L
  var windowTime: Int = 70
  private var microClusters: Array[MicroCluster] = Array.fill(q)(new MicroCluster(Vector.fill[Double](numDimensions)(0.0), Vector.fill[Double](numDimensions)(0.0), 0L, 0L, 0L))
  private var microClusterSW: ArrayBuffer[MicroCluster] = ArrayBuffer[MicroCluster]()
  private var mcInfo: Array[(MicroClusterInfo, Int)] = null

  private var broadcastQ: Broadcast[Int] = null
  private var broadcastMCInfo: Broadcast[Array[(MicroClusterInfo, Int)]] = null

  var initialized = false

  private var useNormalKMeans = false
  private var strKmeans: StreamingKMeans = null



  private var initArr: Array[breeze.linalg.Vector[Double]] = Array()

  /**
    * Random initialization of the q microclusters
    *
    * @param rdd : rdd in use from the incoming DStream
    **/

  private def initRand(rdd: RDD[breeze.linalg.Vector[Double]]): Unit = {
    mcInfo = Array.fill(q)(new MicroClusterInfo(Vector.fill[Double](numDimensions)(rand()), 0.0, 0L)) zip (0 until q)

    val assignations = assignToMicroCluster(rdd, mcInfo)
    updateMicroClusters(assignations)
    var i = 0
    for (mc <- microClusters) {
      mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
      if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x :/ mc.n.toDouble)
      mcInfo(i)._1.setN(mc.getN)
      if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n.toDouble - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n.toDouble)))
      i += 1
    }
    for (mc <- mcInfo) {
      if (mc._1.n == 1)
        mc._1.setRmsd(distanceNearestMC(mc._1.centroid, mcInfo))
    }

    broadcastMCInfo = rdd.context.broadcast(mcInfo)
    initialized = true
  }

  /**
    * Initialization of the q microclusters using the K-Means algorithm
    *
    * @param rdd : rdd in use from the incoming DStream
    **/

  private def initKmeans(rdd: RDD[breeze.linalg.Vector[Double]]): Unit = {
    initArr = initArr ++ rdd.collect
    if (initArr.length >= minInitPoints) {
      val tempRDD = rdd.context.parallelize(initArr)
      val trainingSet = tempRDD.map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))
      val clusters = KMeans.train(trainingSet, q, 10)

      mcInfo = Array.fill(q)(new MicroClusterInfo(Vector.fill[Double](numDimensions)(0), 0.0, 0L)) zip (0 until q)
      for (i <- clusters.clusterCenters.indices) mcInfo(i)._1.setCentroid(DenseVector(clusters.clusterCenters(i).toArray))

      val assignations = assignToMicroCluster(tempRDD, mcInfo)
      updateMicroClusters(assignations)

      var i = 0
      for (mc <- microClusters) {
        mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
        if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x :/ mc.n.toDouble)
        mcInfo(i)._1.setN(mc.getN)
        if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n.toDouble - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n.toDouble)))
        i += 1
      }
      for (mc <- mcInfo) {
        if (mc._1.n == 1)
          mc._1.setRmsd(distanceNearestMC(mc._1.centroid, mcInfo))
      }

      broadcastMCInfo = rdd.context.broadcast(mcInfo)

      initialized = true
    }
  }

  private def initStreamingKmeans(rdd: RDD[breeze.linalg.Vector[Double]]): Unit = {

    if(strKmeans == null) strKmeans = new StreamingKMeans().setK(q).setRandomCenters(numDimensions, 0.0)
    val trainingSet = rdd.map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))

    val clusters = strKmeans.latestModel().update(trainingSet,1.0, "batches")
    if(currentN >= minInitPoints){

      mcInfo = Array.fill(q)(new MicroClusterInfo(Vector.fill[Double](numDimensions)(0), 0.0, 0L)) zip (0 until q)
      for (i <- clusters.clusterCenters.indices) mcInfo(i)._1.setCentroid(DenseVector(clusters.clusterCenters(i).toArray))

      val assignations = assignToMicroCluster(rdd, mcInfo)
      updateMicroClusters(assignations)

      var i = 0
      for (mc <- microClusters) {
        mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
        if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x :/ mc.n.toDouble)
        mcInfo(i)._1.setN(mc.getN)
        if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n.toDouble - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n.toDouble)))
        i += 1
      }
      for (mc <- mcInfo) {
        if (mc._1.n == 1)
          mc._1.setRmsd(distanceNearestMC(mc._1.centroid, mcInfo))
      }

      broadcastMCInfo = rdd.context.broadcast(mcInfo)
      initialized = true
    }

  }
  /**
   * remove old data that expired their time of sliding window
   * update microclusters and mcInfo
   *
   * @return
   */
  def removeOldData(rdd: RDD[Vector[Double]], time: Long) = {
    try {
      val dir = "src/test/resources/snaps"
      if (Files.exists(Paths.get(dir + "/" + time + "SW"))) {
        val snap1: Array[MicroCluster] = microClusters
        val in = new ObjectInputStream(new FileInputStream(dir + "/" + time + "SW"))
        val snap2 = in.readObject().asInstanceOf[Array[MicroCluster]]
        in.close()
        /* val arrs1 = snap1.map(_.getIds)
         val arrs2 = snap2.map(_.getIds)
         val relatingMCs = snap1 zip arrs1.map(a => arrs2.zipWithIndex.map(b => if (b._1.toSet.intersect(a.toSet).nonEmpty) b._2; else -1))
         val mcs = relatingMCs.map { mc =>

           if (!mc._2.forall(_ == -1)) {
             for (id <- mc._2) if (id != -1) {
               mc._1.setCf2x(mc._1.getCf2x :- snap2(id).getCf2x)
               mc._1.setCf1x(mc._1.getCf1x :- snap2(id).getCf1x)
               mc._1.setCf2t(mc._1.getCf2t - snap2(id).getCf2t)
               mc._1.setCf1t(mc._1.getCf1t - snap2(id).getCf1t)
               mc._1.setN(mc._1.getN - snap2(id).getN)
               /*  if(mc._1.getIds.toSet.diff(snap2(id).getIds.toSet).nonEmpty) {
                   mc._1.setIds(mc._1.getIds.toSet.diff(snap2(id).getIds.toSet).toArray)
                 }*/
             }
             mc._1
           } else mc._1

         }*/
        val mcs = snap1.map { mc =>
          for (mc2 <- snap2) {
            if (mc.getIds.intersect(mc2.getIds).nonEmpty) {
              mc.setCf2x(mc.getCf2x :- mc2.getCf2x)
              mc.setCf1x(mc.getCf1x :- mc2.getCf1x)
              mc.setCf2t(mc.getCf2t - mc2.getCf2t)
              mc.setCf1t(mc.getCf1t - mc2.getCf1t)
              mc.setN(mc.getN - mc2.getN)

            }
          }
          mc
        }


        this.microClusters = mcs
        /*  if(time==1){
            println(this.microClusters.map(x=>x.n).mkString(","))
          }*/
        //this.microClusters.foreach(x => print(x.n+","))
        var i = 0
        for (mc <- this.microClusters) {
          mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
          if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x :/ mc.n.toDouble)
          mcInfo(i)._1.setN(mc.getN)
          if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n.toDouble - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n.toDouble)))
          i += 1
        }
        for (mc <- mcInfo) {
          if (mc._1.n == 1)
            mc._1.setRmsd(distanceNearestMC(mc._1.centroid, mcInfo))
        }
        broadcastMCInfo = rdd.context.broadcast(mcInfo)
      }
    }
    catch {
      case ex: IOException => println("Exception while reading files " + ex)
        null
    }
  }
  /**
    * Main method that runs the entire algorithm. This is called every time the
    * Streaming context handles a batch.
    *
    * @param data : data coming from the stream. Each entry has to be parsed as
    *             breeze.linalg.Vector[Double]
    **/

  def run(data: DStream[breeze.linalg.Vector[Double]]): Unit = {
    data.foreachRDD { (rdd, timeS) =>
      //if(getCurrentTime==250) System.exit(0)
      currentN = rdd.count()
      if (currentN != 0) {
        if (initialized) {
          if (this.getCurrentTime + 1 > this.windowTime) {
            val time = this.getCurrentTime + 1 - this.windowTime
            // check if t<(tc-wt)
            val lessThanCurrTime = time-1
            if (lessThanCurrTime > 0){
            //  println(s"currentTime: ${this.getCurrentTime + 1}, windowTime: ${this.windowTime}, diffTime: ${time}}")
            //  removeOldData(rdd, lessThanCurrTime: Long)
            }
          }
          val assignations = assignToMicroCluster(rdd)
          updateMicroClusters(assignations)

          var i = 0
          for (mc <- microClusters) {
            mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
            if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x :/ mc.n.toDouble)
            mcInfo(i)._1.setN(mc.getN)
            if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n.toDouble - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n.toDouble)))
            i += 1
          }
          for (mc <- mcInfo) {
            if (mc._1.n == 1)
              mc._1.setRmsd(distanceNearestMC(mc._1.centroid, mcInfo))
          }

          broadcastMCInfo = rdd.context.broadcast(mcInfo)
        } else {
          minInitPoints match {
            case 0 => initRand(rdd)
            case _ => if(useNormalKMeans) initKmeans(rdd) else initStreamingKmeans(rdd)
          }
        }
      }
      this.time += 1
      this.N += currentN
    }
  }

  /**
    * Method that returns the current array of microclusters.
    *
    * @return Array[MicroCluster]: current array of microclusters
    **/

  def getMicroClusters: Array[MicroCluster] = {
    this.microClusters
  }
  def getMicroClusterSW: Array[MicroCluster] = {
    this.microClusterSW.toArray
  }
  /**
    * Method that returns current time clock unit in the stream.
    *
    * @return Long: current time in stream
    **/

  def getCurrentTime: Long = {
    this.time
  }

  /**
    * Method that returns the total number of points processed so far in
    * the stream.
    *
    * @return Long: total number of points processed
    **/

  def getTotalPoints: Long = {
    this.N
  }

  /**
    * Method that sets if the newly created microclusters due to
    * outliers are able to absorb other outlier points. This is done recursively
    * for all new microclusters, thus disabling these increases slightly the
    * speed of the algorithm but also allows to create overlaping microclusters
    * at this stage.
    *
    * @param ans : true or false
    * @return Class: current class
    **/

  def setRecursiveOutliersRMSDCheck(ans: Boolean): this.type = {
    this.recursiveOutliersRMSDCheck = ans
    this
  }

  /**
    * Changes the K-Means method to use from StreamingKmeans to
    * normal K-Means for the initialization. StreamingKMeans is much
    * faster but in some cases normal K-Means could deliver more
    * accurate initialization.
    *
    * @param ans : true or false
    * @return Class: current class
    **/

  def setInitNormalKMeans(ans: Boolean): this.type = {
    this.useNormalKMeans = ans
    this
  }


  /**
    * Method that sets the m last number of points in a microcluster
    * used to approximate its timestamp (recency value).
    *
    * @param m : m last points
    * @return Class: current class
    **/

  def setM(m: Int): this.type = {
    this.mLastPoints = m
    this
  }

  /**
    * Method that sets the threshold d, used to determine whether a
    * microcluster is safe to delete or not (Tc - d < recency).
    *
    * @param d : threshold
    * @return Class: current class
    **/

  def setDelta(d: Int): this.type = {
    this.delta = d
    this
  }

  /**
    * Method that sets the factor t of RMSDs. A point whose distance to
    * its nearest microcluster is greater than t*RMSD is considered an
    * outlier.
    *
    * @param t : t factor
    * @return Class: current class
    **/

  def setTFactor(t: Double): this.type = {
    this.tFactor = t
    this
  }

  /**
    * Computes the distance of a point to its nearest microcluster.
    *
    * @param vec : the point
    * @param mcs : Array of microcluster information
    * @return Double: the distance
    **/

  private def distanceNearestMC(vec: breeze.linalg.Vector[Double], mcs: Array[(MicroClusterInfo, Int)]): Double = {

    var minDist = Double.PositiveInfinity
    var i = 0
    for (mc <- mcs) {
      val dist = squaredDistance(vec, mc._1.centroid)
      if (dist != 0.0 && dist < minDist) minDist = dist
      i += 1
    }
    scala.math.sqrt(minDist)
  }

  /**
    * Computes the squared distance of two microclusters.
    *
    * @param idx1 : local index of one microcluster in the array
    * @param idx2 : local index of another microcluster in the array
    * @return Double: the squared distance
    **/

  private def squaredDistTwoMCArrIdx(idx1: Int, idx2: Int): Double = {
    squaredDistance(microClusters(idx1).getCf1x :/ microClusters(idx1).getN.toDouble, microClusters(idx2).getCf1x :/ microClusters(idx2).getN.toDouble)
  }

  /**
    * Computes the squared distance of one microcluster to a point.
    *
    * @param idx1  : local index of the microcluster in the array
    * @param point : the point
    * @return Double: the squared distance
    **/

  private def squaredDistPointToMCArrIdx(idx1: Int, point: Vector[Double]): Double = {
    squaredDistance(microClusters(idx1).getCf1x :/ microClusters(idx1).getN.toDouble, point)
  }

  /**
    * Returns the local index of a microcluster for a given ID
    *
    * @param idx0 : ID of the microcluster
    * @return Int: local index of the microcluster
    **/

  private def getArrIdxMC(idx0: Int): Int = {
    var id = -1
    var i = 0
    for (mc <- microClusters) {
      if (mc.getIds(0) == idx0) id = i
      i += 1
    }
    id
  }

  /**
    * Merges two microclusters adding all its features.
    *
    * @param idx1 : local index of one microcluster in the array
    * @param idx2 : local index of one microcluster in the array
    *
    **/

  private def mergeMicroClusters(idx1: Int, idx2: Int): Unit = {

    microClusters(idx1).setCf1x(microClusters(idx1).getCf1x :+ microClusters(idx2).getCf1x)
    microClusters(idx1).setCf2x(microClusters(idx1).getCf2x :+ microClusters(idx2).getCf2x)
    microClusters(idx1).setCf1t(microClusters(idx1).getCf1t + microClusters(idx2).getCf1t)
    microClusters(idx1).setCf2t(microClusters(idx1).getCf2t + microClusters(idx2).getCf2t)
    microClusters(idx1).setN(microClusters(idx1).getN + microClusters(idx2).getN)
    microClusters(idx1).setIds(microClusters(idx1).getIds ++ microClusters(idx2).getIds)

    mcInfo(idx1)._1.setCentroid(microClusters(idx1).getCf1x :/ microClusters(idx1).getN.toDouble)
    mcInfo(idx1)._1.setN(microClusters(idx1).getN)
    mcInfo(idx1)._1.setRmsd(scala.math.sqrt(sum(microClusters(idx1).cf2x) / microClusters(idx1).n.toDouble - sum(microClusters(idx1).cf1x.map(a => a * a)) / (microClusters(idx1).n * microClusters(idx1).n.toDouble)))

  }

  /**
    * Adds one point to a microcluster adding all its features.
    *
    * @param idx1  : local index of the microcluster in the array
    * @param point : the point
    *
    **/

  private def addPointMicroClusters(idx1: Int, point: (Int, Vector[Double])): Unit = {

    microClusters(idx1).setCf1x(microClusters(idx1).getCf1x :+ point._2)
    microClusters(idx1).setCf2x(microClusters(idx1).getCf2x :+ (point._2 :* point._2))
    microClusters(idx1).setCf1t(microClusters(idx1).getCf1t + this.time)
    microClusters(idx1).setCf2t(microClusters(idx1).getCf2t + (this.time * this.time))
    microClusters(idx1).setN(microClusters(idx1).getN + 1)

    mcInfo(idx1)._1.setCentroid(microClusters(idx1).getCf1x :/ microClusters(idx1).getN.toDouble)
    mcInfo(idx1)._1.setN(microClusters(idx1).getN)
    mcInfo(idx1)._1.setRmsd(scala.math.sqrt(sum(microClusters(idx1).cf2x) / microClusters(idx1).n.toDouble - sum(microClusters(idx1).cf1x.map(a => a * a)) / (microClusters(idx1).n * microClusters(idx1).n.toDouble)))

    //----------------- for each of data -----
    if (microClusterSW.nonEmpty) {
      val mc = microClusterSW.find(x => x.getIds(0) == microClusters(idx1).getIds(0)).orNull
      if (mc != null) {
        mc.setCf1x(mc.getCf1x :+ point._2)
        mc.setCf2x(mc.getCf2x :+ (point._2 :* point._2))
        mc.setCf1t(mc.getCf1t + this.time)
        mc.setCf2t(mc.getCf2t + (this.time * this.time))
        mc.setN(mc.getN + 1)
      }
      else {
        microClusterSW.append(new MicroCluster(point._2 :* point._2, point._2, this.time * this.time, this.time, 1, Array(microClusters(idx1).getIds(0))))
      }
    }
    /*microClusterSW(idx1).setCf1x(microClusterSW(idx1).getCf1x :+ point._2)
    microClusterSW(idx1).setCf2x(microClusterSW(idx1).getCf2x :+ (point._2 :* point._2))
    microClusterSW(idx1).setCf1t(microClusterSW(idx1).getCf1t + this.time)
    microClusterSW(idx1).setCf2t(microClusterSW(idx1).getCf2t + (this.time * this.time))
    microClusterSW(idx1).setN(microClusterSW(idx1).getN + 1)
    microClusterSW(idx1).setIds(microClusterSW(idx1).getIds :+ point._1)*/
  }


  /**
    * Deletes one microcluster and replaces it locally with a new point.
    *
    * @param idx   : local index of the microcluster in the array
    * @param point : the point
    *
    **/

  private def replaceMicroCluster(idx: Int, point: (Int, Vector[Double])): Unit = {
    microClusters(idx) = new MicroCluster(point._2 :* point._2, point._2, this.time * this.time, this.time, 1L)
    mcInfo(idx)._1.setCentroid(point._2)
    mcInfo(idx)._1.setN(1L)
    mcInfo(idx)._1.setRmsd(distanceNearestMC(mcInfo(idx)._1.centroid, mcInfo))

    //------------for each data-----------------

    microClusterSW.append(new MicroCluster(point._2 :* point._2, point._2, this.time * this.time, this.time, 1L, microClusters(idx).getIds))
  }


  /**
    * Finds the nearest microcluster for all entries of an RDD.
    *
    * @param rdd    : RDD with points
    * @param mcInfo : Array containing microclusters information
    * @return RDD[(Int, Vector[Double])]: RDD that contains a tuple of the ID of the
    *         nearest microcluster and the point itself.
    *
    **/

  private def assignToMicroCluster(rdd: RDD[Vector[Double]], mcInfo: Array[(MicroClusterInfo, Int)]): RDD[(Int, Vector[Double])] = {
    rdd.map { a =>
      var minDist = Double.PositiveInfinity
      var minIndex = Int.MaxValue
      var i = 0
      for (mc <- mcInfo) {
        val dist = squaredDistance(a, mc._1.centroid)
        if (dist < minDist) {
          minDist = dist
          minIndex = mc._2
        }
        i += 1
      }
      (minIndex, a)
    }
  }

  /**
    * Finds the nearest microcluster for all entries of an RDD, uses broadcast variable.
    *
    * @param rdd    : RDD with points
    * @return RDD[(Int, Vector[Double])]: RDD that contains a tuple of the ID of the
    *         nearest microcluster and the point itself.
    *
    **/
  private def assignToMicroCluster(rdd: RDD[Vector[Double]]) = {
    rdd.map { a =>
      var minDist = Double.PositiveInfinity
      var minIndex = Int.MaxValue
      var i = 0
      for (mc <- broadcastMCInfo.value) {
        val dist = squaredDistance(a, mc._1.centroid)
        if (dist < minDist) {
          minDist = dist
          minIndex = mc._2
        }
        i += 1
      }
      (minIndex, a)
    }
  }

  /**
    * Performs all the operations to maintain the microclusters. Assign points that
    * belong to a microclusters, detects outliers and deals with them.
    *
    * @param assignations : RDD that contains a tuple of the ID of the
    *                     nearest microcluster and the point itself.
    *
    **/

  private def updateMicroClusters(assignations: RDD[(Int, Vector[Double])]): Unit = {

    var dataInAndOut: RDD[(Int, (Int, Vector[Double]), Double)] = null
    var dataIn: RDD[(Int, Vector[Double])] = null
    var dataOut: RDD[((Int, Vector[Double]), Double)] = null


    // Calculate RMSD : برای تعیین حداکثر مرز ,برای خوشه هایی که بیش از یک نقطه دارند
    if (initialized) {
      dataInAndOut = assignations.map { a =>
        val nearMCInfo = broadcastMCInfo.value.find(id => id._2 == a._1).get._1
        val nearDistance = scala.math.sqrt(breeze.linalg.squaredDistance(a._2, nearMCInfo.centroid))
        // tFactor * nearMCInfo.rmsd : تعیین حداکثر مرز به عنوان عاملی از انحراف rms
        if (nearDistance <= tFactor * nearMCInfo.rmsd) (1, a, 0) //If the data point falls within the maximum boundary of the micro-cluster
        else (0, a, nearDistance)
      }
    }

    // Separate data
    if (dataInAndOut != null) {
      dataIn = dataInAndOut.filter(_._1 == 1).map(a => a._2)
      dataOut = dataInAndOut.filter(_._1 == 0).map(a => (a._2, a._3))
    } else dataIn = assignations

    // Compute sums, sums of squares and count points... all by key
    logInfo(s"Processing points")

    // sumsAndSumsSquares -> (key: Int, (sum: Vector[Double], sumSquares: Vector[Double], count: Long ) )
    val sumsAndSumsSquares = timer {
      val aggregateFuntion = (aa: (Vector[Double], Vector[Double], Long), bb: (Vector[Double], Vector[Double], Long)) => (aa._1 :+ bb._1, aa._2 :+ bb._2, aa._3 + bb._3)
      dataIn.mapValues(a => (a, a :* a, 1L)).reduceByKey(aggregateFuntion).collect()
    }


    var totalIn = 0L
    if (microClusterSW.nonEmpty) {
      microClusterSW.clear()
    }
    for (mc <- microClusters) {
      for (ss <- sumsAndSumsSquares) if (mc.getIds(0) == ss._1) {
        mc.setCf1x(mc.cf1x :+ ss._2._1)
        mc.setCf2x(mc.cf2x :+ ss._2._2)
        mc.setN(mc.n + ss._2._3)
        mc.setCf1t(mc.cf1t + ss._2._3 * this.time)
        mc.setCf2t(mc.cf2t + ss._2._3 * (this.time * this.time))
        totalIn += ss._2._3
        val mm = new MicroCluster(ss._2._2,
          ss._2._1,
          ss._2._3 * (this.time * this.time),
          ss._2._3 * this.time,
          ss._2._3,
          Array(ss._1))
        microClusterSW.append(mm)
      }
    }
    logInfo(s"Processing " + (currentN - totalIn) + " outliers")
    timer {
      if (dataOut != null && currentN - totalIn != 0) {
        var mTimeStamp: Double = 0.0
        val recencyThreshold = this.time - delta
        var safeDeleteMC: Array[Int] = Array()
        var keepOrMergeMC: Array[Int] = Array()
        var i = 0


        for (mc <- microClusters) {
          val meanTimeStamp = if (mc.getN > 0) mc.getCf1t.toDouble / mc.getN.toDouble else 0
          val sdTimeStamp = scala.math.sqrt(mc.getCf2t.toDouble / mc.getN.toDouble - meanTimeStamp * meanTimeStamp)

          if (mc.getN < 2 * mLastPoints) mTimeStamp = meanTimeStamp
          else mTimeStamp = Gaussian(meanTimeStamp, sdTimeStamp).icdf(1 - mLastPoints / (2 * mc.getN.toDouble))

          if (mTimeStamp < recencyThreshold || mc.getN == 0) safeDeleteMC = safeDeleteMC :+ i
          else keepOrMergeMC = keepOrMergeMC :+ i

          i += 1
        }

        var j = 0
        var newMC: Array[Int] = Array()


        for (point <- dataOut.collect()) {

          var minDist = Double.PositiveInfinity
          var idMinDist = 0
          if (recursiveOutliersRMSDCheck) for (id <- newMC) {
            val dist = squaredDistPointToMCArrIdx(id,point._1._2)
            if (dist < minDist) {
              minDist = dist
              idMinDist = id
            }

          }

          if (scala.math.sqrt(minDist) <= tFactor * mcInfo(idMinDist)._1.rmsd) addPointMicroClusters(idMinDist, point._1)
          else if (safeDeleteMC.lift(j).isDefined) {
            replaceMicroCluster(safeDeleteMC(j), point._1)
            newMC = newMC :+ safeDeleteMC(j)
            j += 1
          } else {
            var minDist = Double.PositiveInfinity
            var idx1 = 0
            var idx2 = 0

            for (a <- keepOrMergeMC.indices)
              for (b <- (0 + a) until keepOrMergeMC.length) {
                var dist = Double.PositiveInfinity
                if (keepOrMergeMC(a) != keepOrMergeMC(b)) dist = squaredDistance(mcInfo(keepOrMergeMC(a))._1.centroid, mcInfo(keepOrMergeMC(b))._1.centroid)
                if (dist < minDist) {
                  minDist = dist
                  idx1 = keepOrMergeMC(a)
                  idx2 = keepOrMergeMC(b)
                }
              }
            mergeMicroClusters(idx1, idx2)
            replaceMicroCluster(idx2, point._1)
            newMC = newMC :+ idx2
          }

        }

      }
    }
  }

  // END OF MODEL
}


/**
  * Object complementing the MicroCluster Class to allow it to create
  * new IDs whenever a new instance of it is created.
  *
  **/

private object MicroCluster extends Serializable {
  private var current = -1

  private def inc = {
    current += 1
    current
  }
}

/**
  * Packs the microcluster object and its features in one single class
  *
  **/

protected class MicroCluster(
                              var cf2x: breeze.linalg.Vector[Double],
                              var cf1x: breeze.linalg.Vector[Double],
                              var cf2t: Long,
                              var cf1t: Long,
                              var n: Long,
                              var ids: Array[Int]) extends Serializable {

  def this(cf2x: breeze.linalg.Vector[Double], cf1x: breeze.linalg.Vector[Double], cf2t: Long, cf1t: Long, n: Long) = this(cf2x, cf1x, cf2t, cf1t, n, Array(MicroCluster.inc))

  def setCf2x(cf2x: breeze.linalg.Vector[Double]): Unit = {
    this.cf2x = cf2x
  }

  def getCf2x: breeze.linalg.Vector[Double] = {
    this.cf2x
  }

  def setCf1x(cf1x: breeze.linalg.Vector[Double]): Unit = {
    this.cf1x = cf1x
  }

  def getCf1x: breeze.linalg.Vector[Double] = {
    this.cf1x
  }

  def setCf2t(cf2t: Long): Unit = {
    this.cf2t = cf2t
  }

  def getCf2t: Long = {
    this.cf2t
  }

  def setCf1t(cf1t: Long): Unit = {
    this.cf1t = cf1t
  }

  def getCf1t: Long = {
    this.cf1t
  }

  def setN(n: Long): Unit = {
    this.n = n
  }

  def getN: Long = {
    this.n
  }

  def setIds(ids: Array[Int]): Unit = {
    this.ids = ids
  }

  def getIds: Array[Int] = {
    this.ids
  }
}


/**
  * Packs some microcluster information to reduce the amount of data to be
  * broadcasted.
  *
  **/

private class MicroClusterInfo(
                                var centroid: breeze.linalg.Vector[Double],
                                var rmsd: Double,
                                var n: Long) extends Serializable {

  def setCentroid(centroid: Vector[Double]): Unit = {
    this.centroid = centroid
  }

  def setRmsd(rmsd: Double): Unit = {
    this.rmsd = rmsd
  }

  def setN(n: Long): Unit = {
    this.n = n
  }
}

