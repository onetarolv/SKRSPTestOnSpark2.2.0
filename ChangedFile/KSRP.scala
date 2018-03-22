package org.apache.spark

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.Utils
import org.apache.spark.util.random.SamplingUtils

import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering
import scala.reflect.ClassTag
import scala.util.Random

/**
 * Created by DELL_PC on 2017/12/20.
 */
class KSRP[K : Ordering : ClassTag, V](
  partitions: Int,
  rdd: RDD[_ <: Product2[K, V]],
  private var ascending: Boolean = true)
  extends Partitioner with Logging{

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")

  private var ordering = implicitly[Ordering[K]]
  private val SAMPLE_RATE = rdd.conf.getDouble("spark.partitioner.samplerate",0.2)
  private val SKEW_DEGREE = rdd.conf.getDouble("spark.partitioner.skewdegree",1.2)
  private val SAMPLE_SIZE_LOWER_BOUND = rdd.conf.getInt("spark.partitioner.samplesize.lowerbound",1000)
  private val SAMPLE_ALPHA = rdd.conf.getDouble("spark.partitioner.shash.alpha", 0.07)
  private val SAMPLE_TIMES = rdd.conf.getDouble("spark.partitioner.samplesize.times", 1000)
  private val NODES_NUMBER = rdd.conf.getInt("spark.nodenumber", 8)
  private val SAMPLE_FRACTION = 1.0 min ((rdd.conf.getDouble("spark.partitioner.fraction", 1.0)) * (NODES_NUMBER.toDouble/rdd.getNumPartitions))
  private val PARTITIONER = rdd.conf.get("spark.partitioner.class", "skrsp")

  // An array of upper bounds for the first (partitions - 1) partitions
  private var rangeBounds: Array[(K, Double)] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      println("rejrange")
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      val sampleSize = math.min(SAMPLE_SIZE_LOWER_BOUND * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizeLowerBound = math.ceil(sampleSize / rdd.partitions.length).toInt
      val sketched = {
        if (rdd.conf.get("spark.partitioner.class", "skrsp").equals("skrsp-r"))
          KRHP.sketchReservoir(rdd.map(_._1), sampleSizeLowerBound, SAMPLE_FRACTION)
        else
          KRHP.sketchRejection(rdd.map(_._1), sampleSizeLowerBound, (sampleSizeLowerBound * SAMPLE_TIMES).toInt, SAMPLE_ALPHA, SAMPLE_RATE, SAMPLE_FRACTION)
      }
      if (sketched.isEmpty) {
        Array.empty
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        //val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Double)]
        sketched.foreach { case (id, weight, sample, time) =>
          for ((key, freq) <- sample) {
            val kweight = freq * weight
            candidates += ((key, kweight))
          }
        }
          KSRP.determineBounds(candidates, partitions)
     }
    }
  }

  def numPartitions: Int = rangeBounds.length + 1

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    var partition = 0
    //    if (rangeBounds.length <= 128) {
    // If we have less than 128 partitions naive search
    while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition)._1)) {
      partition += 1
    }

    if(partition < rangeBounds.length && ordering.equiv(k, rangeBounds(partition)._1)){
      var (boundkey, ratio) = rangeBounds(partition)
      if(ratio != 1.0){
        val random = new Random().nextDouble()
        while(partition < rangeBounds.length && ordering.equiv(boundkey, rangeBounds(partition)._1) && random > ratio){
          partition += 1
          if(partition < rangeBounds.length)
            ratio = ratio + rangeBounds(partition)._2
        }
      }
    }
    if(partition > rangeBounds.length){
      partition = rangeBounds.length
    }
    partition

  }

  override def equals(other: Any): Boolean = other match {
    case r: KSRP[_, _] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode
    result
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeBoolean(ascending)
        out.writeObject(ordering)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[Array[(K, Double)]])
          stream.writeObject(rangeBounds)
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        ascending = in.readBoolean()
        ordering = in.readObject().asInstanceOf[Ordering[K]]
        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[Array[(K, Double)]]]()
          rangeBounds = ds.readObject[Array[(K, Double)]]()
        }
    }
  }
}

private[spark] object KSRP {
  /**
   * Sketches the input RDD via reservoir sampling on each partition.
   *
   * @param rdd the input RDD to sketch
   * @param sampleSizeLowerBound min sample size per partition
   * @return (total number of items, an array of (partitionId, number of items, sample))
   */
  def sketch[K : ClassTag](
                            rdd: RDD[K],
                            sampleSizeLowerBound: Int,
                            sampleSizeUpperBound: Int,
                            alpha: Double,
                            sampleRate: Double):  Array[(Double, Array[K])]= {
    val shift = rdd.id
    val iterrdd = rdd.mapPartitions(iter => Iterator(iter))
    val sizes = iterrdd.map(_.size)

    val sketched = iterrdd.zip(sizes).map(iter_size => {
      val iter = iter_size._1
      val size = iter_size._2
      val (sample, rate) = SamplingUtils.rejectionSampleAndCount(
        iter, sampleSizeLowerBound, sampleSizeUpperBound, alpha, size, sampleRate)
      (rate, sample)
    }).collect()
    sketched
  }

  /**
   * Determines the bounds for range partitioning from candidates with weights indicating how many
   * items each represents. Usually this is 1 over the probability used to sample this candidate.
   *
   * @param candidates unordered candidates with weights
   * @param partitions number of partitions
   * @return selected bounds
   */
  def determineBounds[K : Ordering : ClassTag](
                                                candidates: ArrayBuffer[(K, Double)],
                                                partitions: Int): Array[(K, Double)] = {
    val ordering = implicitly[Ordering[K]]
    val ordered = candidates.sortBy(_._1)
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[(K, Double)]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    var preKey = ordered(0)._1
    var curKeyWeight = ordered(0)._2
    var rest = step
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      curKeyWeight = 0
      while(i < numCandidates && ordering.equiv(key, ordered(i)._1)){
        curKeyWeight += ordered(i)._2
        i = i + 1
      }
      i = i - 1
      if(rest <= curKeyWeight){
        ordered(i) = (key, (curKeyWeight - rest).toFloat)
        bounds += ((key, rest/curKeyWeight))
        j += 1
        rest = step
      } else{
        rest = rest - curKeyWeight
        i = i + 1
      }
    }
    bounds.toArray
  }

  def determineBoundsByLIBRA[K : ClassTag](
                                         clusterWeights: Array[(K, Double)],
                                         partitions: Int): Array[Int] = {
    println("use LIBRA !!")
    def weight(a: Int, b: Int): Double = {
      var w = 0.0
      for( l <- a  to b) {
        w += clusterWeights(l)._2
      }
      w
    }
    val keyNumber = clusterWeights.length
    def libra(i: Int, j: Int): (Double, ArrayBuffer[Int]) ={
      if(j == 0 && i != 0)
        return (weight(0, i - 1), ArrayBuffer.empty)
      if(i == j) {
        var k = 0
        var Max = 0.0
        val bounds = new ArrayBuffer[Int]
        while(k <= i - 1) {
          bounds.append(k)
          Max = clusterWeights(k)._2 max Max
        }
        return (Max, bounds)
      }
      if(i <= 0 || i < j || j <= 0)
        return (Double.MinValue, ArrayBuffer.empty)
      var minMax = Double.MaxValue
      var split = 0
      var bounds = ArrayBuffer.empty[Int]
      var arrayBuffer = new ArrayBuffer[Int]()
      var curWeight = 0.0
      var kdx = i - 2
      while(kdx + 1 >= j  && j - 1 >= 0 & kdx >= 0) {
        val (cost, curBounds) = libra( kdx + 1, j - 1)
        curWeight= curWeight + clusterWeights(kdx + 1)._2
        val curMax = cost max curWeight
        println("curMax = " + curMax + "; kdx = " + kdx + "; j = " + j + "; i = " + i + "; curWeight = " + curWeight + "; cost = " + cost)
        if(curMax <= minMax){
          minMax = curMax
          split = kdx
          bounds = curBounds
        }
        kdx -= 1
      }
      bounds.append(split)
      (minMax, bounds)
    }
    val (maxWeight, bounds) = libra(keyNumber, partitions-1)
    bounds.toArray
  }

//  def main (args: Array[String]){
//    val keys = Array(("a", 11.0), ("b", 3.0), ("c",9.0), ("d", 1.0), ("e", 7.0), ("f", 3.0), ("g",4.0), ("h", 8.0) )
//    val (maxWeight, bounds) = reAssignKeysByLIBRA(keys, 4)
//    println("maxWeight = " + maxWeight)
//    println("bounds = [" + bounds.mkString(", ") + " ]")
//  }
}