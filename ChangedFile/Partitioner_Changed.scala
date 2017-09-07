/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.internal.Logging
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.hashing.byteswap32

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{CollectionsUtils, Utils}
import org.apache.spark.util.random.SamplingUtils

/**
 * An object that defines how the elements in a key-value pair RDD are partitioned by key.
 * Maps each key to a partition ID, from 0 to `numPartitions - 1`.
 */
abstract class Partitioner extends Serializable {
  def numPartitions: Int
  def getPartition(key: Any): Int
}

object Partitioner {
  /**
   * Choose a partitioner to use for a cogroup-like operation between a number of RDDs.
   *
   * If any of the RDDs already has a partitioner, choose that one.
   *
   * Otherwise, we use a default HashPartitioner. For the number of partitions, if
   * spark.default.parallelism is set, then we'll use the value from SparkContext
   * defaultParallelism, otherwise we'll use the max number of upstream partitions.
   *
   * Unless spark.default.parallelism is set, the number of partitions will be the
   * same as the number of partitions in the largest upstream RDD, as this should
   * be least likely to cause out-of-memory errors.
   *
   * We use two method parameters (rdd, others) to enforce callers passing at least 1 RDD.
   */
  def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
    val rdds = (Seq(rdd) ++ others)
    val hasPartitioner = rdds.filter(_.partitioner.exists(_.numPartitions > 0))
    if (hasPartitioner.nonEmpty) {
      hasPartitioner.maxBy(_.partitions.length).partitioner.get
    } else {
      if (rdd.context.conf.contains("spark.default.parallelism")) {
        new HashPartitioner(rdd.context.defaultParallelism)
      } else {
        new HashPartitioner(rdds.map(_.partitions.length).max)
      }
    }
  }
}

/** KRHP-R: key reassigning hash partition algorithm based on Reservoir Sampling */
class BalanceHashPartitionerByReservoir[K : Ordering : ClassTag, V](
   partitions: Int,
   self: RDD[_ <: Product2[K, V]],
   others: RDD[_<: Product2[K, V]]*)
  extends Partitioner{
  var skewKeyNum : Int = 0
  private var _partitions = partitions
  def numPartitions = _partitions

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    val hashPart = getHashPartition(k)
    val hashmap = strategy.get(hashPart)
    if(hashmap.nonEmpty){
      val newPartition = hashmap.get.get(k)
      if(newPartition.nonEmpty){
        return newPartition.get
      }else{
        return hashPart
      }
    }else{
      return hashPart
    }
  }

  def getHashPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: BalanceHashPartitionerByReservoir[_,_] =>
      h.strategy.sameElements(strategy)
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
  val rdds = (Seq(self) ++ others)

  def computeRDDToKeyWeight(rdd: RDD[_ <: Product2[K, V]], sampleSizePerPartition: Int, sampleSize: Double): ArrayBuffer[(K, Float)] ={
    // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
    val starttime = System.currentTimeMillis()
    val (numItems, sketched) = BalanceHashPartitionerByReservoir.sketch(rdd.map(_._1), sampleSizePerPartition)
    val totaltime = System.currentTimeMillis() - starttime
    if (numItems == 0L) {
      ArrayBuffer.empty
    } else {
      // If a partition contains much more than the average number of items, we re-sample from it
      // to ensure that enough items are collected from that partition.
      val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
      val keyToNum = ArrayBuffer.empty[(K, Float)]
      sketched.foreach { case (idx, n, sample) =>
        // The weight is 1 over the sampling probability.
        val weight = (n.toDouble / sample.length).toFloat
        for (key <- sample) {
          keyToNum += ((key, weight))
        }
      }
      keyToNum
    }
  }
  private var SKEW_DEGREE = self.conf.getDouble("spark.partitioner.skewdegree",1.2)
  private var SAMPLE_SIZE_LOWER_BOUND = self.conf.getInt("spark.partitioner.samplesize.lowerbound",20)


  private var strategy: mutable.HashMap[Int, mutable.HashMap[K, Int]] = {
    if (_partitions <= 1){
      mutable.HashMap.empty
    } else {
      val keyToNum = ArrayBuffer.empty[(K, Float)]
      for(rdd <- rdds){
        val sampleSize = math.min(SAMPLE_SIZE_LOWER_BOUND * _partitions, 1e6)
        // Assume the input partitions are roughly balanced and over-sample a little bit.
        val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.getNumPartitions).toInt

        keyToNum ++= computeRDDToKeyWeight(rdd, sampleSizePerPartition, sampleSize)
      }
      BalanceHashPartitionerByReservoir.reAssignKeyToPartition(keyToNum.toArray,_partitions,SKEW_DEGREE)
    }
  }

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeInt(_partitions)
        out.writeInt(SAMPLE_SIZE_LOWER_BOUND)
        out.writeDouble(SKEW_DEGREE)
        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[mutable.HashMap[Int, mutable.HashMap[K, Int]]])
          stream.writeObject(strategy)
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        _partitions = in.readInt()
        SAMPLE_SIZE_LOWER_BOUND = in.readInt()
        SKEW_DEGREE = in.readDouble()
        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[mutable.HashMap[Int, mutable.HashMap[K, Int]]]]()
          strategy = ds.readObject[mutable.HashMap[Int, mutable.HashMap[K, Int]]]()
        }
    }
  }
}
object BalanceHashPartitionerByReservoir {

  def getHashPartition(key: Any, numPartitions: Int): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }
  def sketch[K : ClassTag](
                            rdd: RDD[K],
                            sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])]) = {
    val shift = rdd.id
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }.collect()
    val numItems = sketched.map(_._2).sum
    (numItems, sketched)
  }


  def computeKeyToTotalWeight[K: Ordering : ClassTag](
   keyToNumber: Array[(K, Float)]): (Array[(K, Float)], Double) = {
    val ordering = implicitly[Ordering[K]]
    val keyToTotalNums = new ArrayBuffer[(K, Float)]()

    val starttime = System.currentTimeMillis()
    val ordered = keyToNumber.sortBy(_._1)
    var preKey = ordered(0)._1
    var totalKeyWeght: Double = 0
    var oneKeyWeight: Float = 0
    for((key, weight) <- ordered){
      if( !key.equals(preKey)){
        keyToTotalNums += ((preKey, oneKeyWeight))
        totalKeyWeght = totalKeyWeght + oneKeyWeight
        oneKeyWeight = weight
      }else{
        oneKeyWeight = oneKeyWeight + weight
      }
      preKey = key
    }
    if(oneKeyWeight !=0 ){
      totalKeyWeght = totalKeyWeght + oneKeyWeight
      keyToTotalNums += ((preKey, oneKeyWeight))
    }

    (keyToTotalNums.toArray, totalKeyWeght)

  }
  def reAssignKeyToPartition[K: Ordering : ClassTag](
  keyToNumber: Array[(K, Float)],
  partitions: Int,
  skewDegree:Double): mutable.HashMap[Int, mutable.HashMap[K, Int]] = {//(mutable.HashMap[K, Int], Array[Int]) = {

    val ordering = implicitly[Ordering[K]]
    // get the weights of keys
    val (keyToTotalWeight, totalWeights) = computeKeyToTotalWeight(keyToNumber)
    val keyNum = keyToTotalWeight.size

    val strategy = new mutable.HashMap[Int, mutable.HashMap[K, Int]]()
    val step = totalWeights / partitions
    val avgsize = totalWeights / keyNum

    var reduceTupleNum = new Array[(Int, Float)](partitions)
    val partToKey = new Array[ArrayBuffer[(K, Float)]](partitions)

    for (i <- 0 until partitions) {
      reduceTupleNum(i) = (i, 0)
      partToKey(i) = new ArrayBuffer[(K, Float)]()
    }
    //use HashPartitioner, caculate tuples number of every partition
    for(i <- 0 until keyNum){
      val (key, weights) = keyToTotalWeight(i)
      val p = getHashPartition(key, partitions)
      partToKey(p).append((key, weights))
      reduceTupleNum(p) = (p, reduceTupleNum(p)._2 + weights)
    }
    implicit val reduceordering = new Ordering[Float] {
      override def compare(a: Float, b: Float) = a.compare(b)
    }
    implicit val skewordering = new Ordering[Float] {
      override def compare(a: Float, b: Float) = b.compare(a)
    }
    //get the average number for one partition
    val avgNumPerPart = reduceTupleNum.map(_._2).sum/partitions
    val threshold = avgNumPerPart * skewDegree
    var reAssignKeys = new ArrayBuffer[(K, Float, Int)]()
    for(i <- 0 until partitions){
      val (part, curweights) = reduceTupleNum(i)
      if(curweights > threshold){
        // des by weights
        strategy(part) = new mutable.HashMap[K, Int]()
        var rest = avgNumPerPart
        val keys = partToKey(i).toArray.sortBy(_._2)(skewordering)
        for((key, weights) <- keys){
          if(rest >= weights){
            rest = rest - weights
          }else{
            reAssignKeys.append((key,weights,i))
          }
        }
        reduceTupleNum(i) = (part, avgNumPerPart - rest)
      }
    }
    reduceTupleNum = reduceTupleNum.sortBy(_._2)(reduceordering)
    //record the keys which belong to over-size partitions des
    val reAssignKeyNum = reAssignKeys.length

    //fill every partitions using bigger tuple clusters, but every partitions is smaller than avgNumPerPart
    for(i <- 0 until partitions){
      val (part, curweight) = reduceTupleNum(i)
      var rest = avgNumPerPart - curweight
      var j = 0
      while(j < reAssignKeyNum){
        val (key, weights,hashPart) = reAssignKeys(j)
        if(weights > 0 && weights < rest){
          rest = rest - weights
          //add [key, new partition] to strategy
          strategy(hashPart)(key) = part
          reAssignKeys(j) = (key, 0, hashPart)
        }
        j = j + 1
      }
      reduceTupleNum(i) = (part, avgNumPerPart - rest)
    }
    //asc
    reduceTupleNum = reduceTupleNum.sortBy(_._2)(reduceordering)
    //des
    reAssignKeys = reAssignKeys.filter(_._2 != 0).sortBy(_._2)(skewordering)

    var i = 0
    for((key, weights, hashPart) <- reAssignKeys){
      val (part, curweight) = reduceTupleNum(i)
      //println(i + " get " + weights + " before " +curweight)
      reduceTupleNum(i)= (part, curweight + weights)
      strategy(hashPart)(key) = part
      i = i + 1
    }
    return strategy
  }
}

/** KSRP-R: key splitting range partition algorithm based on Reservoir Sampling */
class SplitRangePartitionerByReservoir[K : Ordering : ClassTag, V](
    partitions: Int,
    rdd: RDD[_ <: Product2[K, V]],
    private var ascending: Boolean = true)
  extends Partitioner with Logging{
  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")

  private var ordering = implicitly[Ordering[K]]
  private val SAMPLE_SIZE_LOWER_BOUND = rdd.conf.getInt("spark.partitioner.samplesize.lowerbound",1000)

  // An array of upper bounds for the first (partitions - 1) partitions
  private var rangeBounds: Array[(K, Double)] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      val sampleSize = math.min(SAMPLE_SIZE_LOWER_BOUND * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizeLowerBound = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizeLowerBound)
      if (numItems == 0L) {
        Array.empty
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, sample) =>
          logInfo("========== sample items: " + sample.size + " ===========")
          if (fraction * n > sampleSizeLowerBound) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.length).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        SplitRangePartitionerByReservoir.determineBounds(candidates, partitions)
      }
    }
  }

  def numPartitions: Int = rangeBounds.length + 1

  private var binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K]

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

    if (ascending) {
      return partition
    } else {
      return rangeBounds.length - partition
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: SplitRangePartitionerByReservoir[_, _] =>
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
          stream.writeObject(scala.reflect.classTag[Array[(K,Double)]])
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
          implicit val classTag = ds.readObject[ClassTag[Array[(K,Double)]]]()
          rangeBounds = ds.readObject[Array[(K, Double)]]()
        }
    }
  }
}

private[spark] object SplitRangePartitionerByReservoir {
  /**
   * Sketches the input RDD via reservoir sampling on each partition.
   *
   * @param rdd the input RDD to sketch
   * @param sampleSizePerPartition max sample size per partition
   * @return (total number of items, an array of (partitionId, number of items, sample))
   */
  def sketch[K : ClassTag](
    rdd: RDD[K],
    sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])]) = {
    val shift = rdd.id
    // val classTagK = classTag[K] // to avoid serializing the entire partitioner object
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }.collect()
    val numItems = sketched.map(_._2).sum
    (numItems, sketched)
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
                                                candidates: ArrayBuffer[(K, Float)],
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
      //put the rest weight in the last position of key in ordered
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
}

class SplitRangePartitionerByRejection[K : Ordering : ClassTag, V](
  partitions: Int,
  rdd: RDD[_ <: Product2[K, V]],
  private var ascending: Boolean = true)
  extends Partitioner{
  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")

  private var ordering = implicitly[Ordering[K]]
  private val SAMPLE_RATE = rdd.conf.getDouble("spark.partitioner.samplerate",1e-4)
  private val SKEW_DEGREE = rdd.conf.getDouble("spark.partitioner.skewdegree",1.2)
  private val SAMPLE_SIZE_LOWER_BOUND = rdd.conf.getInt("spark.partitioner.samplesize.lowerbound",100)
  private val SAMPLE_ALPHA = rdd.conf.getDouble("spark.partitioner.shash.alpha", 0.07)
  private val SAMPLE_TIMES = rdd.conf.getDouble("spark.partitioner.samplesize.times", 10000)


  // An array of upper bounds for the first (partitions - 1) partitions
  private var rangeBounds: Array[(K, Double)] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      val sampleSize = math.min(SAMPLE_SIZE_LOWER_BOUND * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizeLowerBound = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val sketched= SplitRangePartitionerByRejection.sketch(rdd.map(_._1), sampleSizeLowerBound, (sampleSizeLowerBound * SAMPLE_TIMES).toInt, SAMPLE_ALPHA, SAMPLE_RATE)
      if (sketched.isEmpty) {
        Array.empty
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        //val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (fraction, sample) =>
          // The weight is 1 over the sampling probability.
          val weight = (1.0 / fraction).toFloat
          for (key <- sample) {
            candidates += ((key, weight))
          }
        }
        SplitRangePartitionerByRejection.determineBounds(candidates, partitions)
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

    if (ascending) {
      return partition
    } else {
      return rangeBounds.length - partition
    }

  }

  override def equals(other: Any): Boolean = other match {
    case r: SplitRangePartitionerByRejection[_, _] =>
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

private[spark] object SplitRangePartitionerByRejection {
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
  candidates: ArrayBuffer[(K, Float)],
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
      //put the rest weight in the last position of key in ordered
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
}

class BalanceHashPartitionerByRejection[K : Ordering : ClassTag, V](
 partitions: Int,
 self: RDD[_ <: Product2[K, V]],
 others: RDD[_<: Product2[K, V]]*)
  extends Partitioner {
  private var _partitions = partitions
  def numPartitions = _partitions
  private var ordering = implicitly[Ordering[K]]
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    val hashPart = getHashPartition(k)
    val hashmap = strategy.get(hashPart)
    if(hashmap.nonEmpty){
      val newPartition = hashmap.get.get(k)
      if(newPartition.nonEmpty){
        return newPartition.get
      }else{
        return hashPart
      }
    }else{
      return hashPart
    }
    //println("assign " + k + "to partition " + partition)
  }

  def getHashPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: BalanceHashPartitionerByRejection[_, _] =>
      h.strategy.sameElements(strategy)
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions

  val rdds = (Seq(self) ++ others)
  //  val defaultpartition : Int = {
  //    if (partitions > 1) {
  //      partitions
  //    } else {
  //      if (self.context.conf.contains("spark.default.parallelism")) {
  //        self.getNumPartitions
  //      } else {
  //        rdds.map(_.partitions.length).max
  //      }
  //    }
  //  }

  def computeRDDToKeyWeight(rdd: RDD[_ <: Product2[K, V]], sampleSizeLowerBound: Int, sampleSizeUpperBound: Int, alpha: Double, sampleRate: Double): ArrayBuffer[(K, Float)] ={
    // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
    val starttime = System.currentTimeMillis()
    val sketched = BalanceHashPartitionerByRejection.sketch(rdd.map(_._1), sampleSizeLowerBound, sampleSizeUpperBound, alpha, sampleRate)
    val totaltime = System.currentTimeMillis() - starttime
    if (sketched.isEmpty) {
      ArrayBuffer.empty
    } else {

      val keyToNum = ArrayBuffer.empty[(K, Float)]
      sketched.foreach { case (fraction, sample) =>
        // The weight is 1 over the sampling probability.
        val weight = (1 / fraction).toFloat
        for (key <- sample) {
          keyToNum += ((key, weight))
        }
      }
      keyToNum
    }
  }
  private var SAMPLE_RATE = self.conf.getDouble("spark.partitioner.samplerate",0.5e-4)
  private var SKEW_DEGREE = self.conf.getDouble("spark.partitioner.skewdegree",1.2)
  private var SAMPLE_SIZE_LOWER_BOUND = self.conf.getInt("spark.partitioner.samplesize.lowerbound",100)
  private var SAMPLE_ALPHA = self.conf.getDouble("spark.partitioner.shash.alpha", 0.07)
  private var SAMPLE_TIMES = self.conf.getDouble("spark.partitioner.samplesize.times", 10000.0)

  private var strategy : mutable.HashMap[Int, mutable.HashMap[K, Int]] = {
    val ordering = implicitly[Ordering[K]]
    if (partitions <= 1 || ordering==null){
      mutable.HashMap.empty//, Array.empty)
    } else {

      val keyToNum = ArrayBuffer.empty[(K, Float)]
      for(rdd <- rdds){
        val sampleSize = math.min(SAMPLE_SIZE_LOWER_BOUND * partitions, 1e6)
        // Assume the input partitions are roughly balanced and over-sample a little bit.
        // the sample size is the lower boundary, the real smaple size will be caculated later.
        val sampleSizeLowerBound = math.ceil(3.0 * sampleSize / rdd.getNumPartitions).toInt

        keyToNum ++= computeRDDToKeyWeight(rdd, sampleSizeLowerBound, (sampleSizeLowerBound * SAMPLE_TIMES).toInt, SAMPLE_ALPHA, SAMPLE_RATE)
      }
      BalanceHashPartitionerByRejection.reAssignKeyToPartition(keyToNum.toArray,partitions,SKEW_DEGREE)
    }
  }


  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeInt(_partitions)
        out.writeDouble(SAMPLE_ALPHA)
        out.writeDouble(SAMPLE_RATE)
        out.writeInt(SAMPLE_SIZE_LOWER_BOUND)
        out.writeDouble(SAMPLE_TIMES)
        out.writeDouble(SKEW_DEGREE)
        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[mutable.HashMap[Int, mutable.HashMap[K, Int]]])
          stream.writeObject(strategy)
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        _partitions = in.readInt()
        SAMPLE_ALPHA = in.readDouble()
        SAMPLE_RATE = in.readDouble()
        SAMPLE_SIZE_LOWER_BOUND = in.readInt()
        SAMPLE_TIMES = in.readDouble()
        SKEW_DEGREE = in.readDouble()
        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[mutable.HashMap[Int, mutable.HashMap[K, Int]]]]()
          strategy = ds.readObject[mutable.HashMap[Int, mutable.HashMap[K, Int]]]()
        }
    }
  }

}
object BalanceHashPartitionerByRejection{
  def getHashPartition(key: Any, numPartitions: Int): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  def sketch[K : ClassTag](
                            rdd: RDD[K],
                            sampleSizeLowerBound: Int,
                            sampleSizeUpperBound: Int,
                            alpha: Double,
                            sampleRate: Double):  Array[(Double, Array[K])]= {
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



  def computeKeyToTotalWeight[K: Ordering : ClassTag](
                                                       keyToNumber: Array[(K, Float)]): (Array[(K, Float)], Double) = {
    val ordering = implicitly[Ordering[K]]
    val keyToTotalNums = new ArrayBuffer[(K, Float)]()

    val starttime = System.currentTimeMillis()
    val ordered = keyToNumber.sortBy(_._1)

    var preKey = ordered(0)._1
    var totalKeyWeght: Double = 0
    var oneKeyWeight: Float = 0
    for((key, weight) <- ordered){
      if( !key.equals(preKey)){
        //不是第一个并且与前一个不同
        keyToTotalNums += ((preKey, oneKeyWeight))
        totalKeyWeght = totalKeyWeght + oneKeyWeight
        oneKeyWeight = weight
      }else{
        oneKeyWeight = oneKeyWeight + weight
        //keyToTotalNums += (totalKeyNum + keyToTotalNums.last._2)
      }
      preKey = key
    }
    if(oneKeyWeight !=0 ){
      totalKeyWeght = totalKeyWeght + oneKeyWeight
      keyToTotalNums += ((preKey, oneKeyWeight))
    }
    (keyToTotalNums.toArray, totalKeyWeght)
  }

  def reAssignKeyToPartition[K: Ordering : ClassTag](
  keyToNumber: Array[(K, Float)],
  partitions: Int,
  skewDegree:Double): mutable.HashMap[Int, mutable.HashMap[K, Int]] = {//(mutable.HashMap[K, Int], Array[Int]) = {
    val ordering = implicitly[Ordering[K]]
    // get the weights of keys

    val (keyToTotalWeight, totalWeights) = computeKeyToTotalWeight(keyToNumber)
    val keyNum = keyToTotalWeight.size

    val strategy = new mutable.HashMap[Int, mutable.HashMap[K, Int]]()
    val step = totalWeights / partitions
    val avgsize = totalWeights / keyNum

    var reduceTupleNum = new Array[(Int, Float)](partitions)
    val partToKey = new Array[ArrayBuffer[(K, Float)]](partitions)

    for (i <- 0 until partitions) {
      reduceTupleNum(i) = (i, 0)
      partToKey(i) = new ArrayBuffer[(K, Float)]()
    }
    //use HashPartitioner, caculate tuples number of every partition
    for(i <- 0 until keyNum){
      val (key, weights) = keyToTotalWeight(i)
      val p = getHashPartition(key, partitions)
      partToKey(p).append((key, weights))
      reduceTupleNum(p) = (p, reduceTupleNum(p)._2 + weights)
    }
    implicit val reduceordering = new Ordering[Float] {
      override def compare(a: Float, b: Float) = a.compare(b)
    }
    implicit val skewordering = new Ordering[Float] {
      override def compare(a: Float, b: Float) = b.compare(a)
    }
    //get the average number for one partition
    val avgNumPerPart = reduceTupleNum.map(_._2).sum/partitions
    val threshold = avgNumPerPart * skewDegree
    var reAssignKeys = new ArrayBuffer[(K, Float, Int)]()
    for(i <- 0 until partitions){
      val (part, curweights) = reduceTupleNum(i)
      if(curweights > threshold){
        // des by weights
        strategy(part) = new mutable.HashMap[K, Int]()
        var rest = avgNumPerPart
        val keys = partToKey(i).toArray.sortBy(_._2)(skewordering)
        for((key, weights) <- keys){
          if(rest >= weights){
            rest = rest - weights
          }else{
            reAssignKeys.append((key,weights,i))
          }
        }
        reduceTupleNum(i) = (part, avgNumPerPart - rest)
      }
    }
    reduceTupleNum = reduceTupleNum.sortBy(_._2)(reduceordering)
    //reAssignParts.foreach(println)
    //record the keys which belong to over-size partitions des
    //reAssignKeys = reAssignKeys.sortBy(_._2)(skewordering)
    val reAssignKeyNum = reAssignKeys.length

    //fill every partitions using bigger tuple clusters, but every partitions is smaller than avgNumPerPart
    for(i <- 0 until partitions){
      val (part, curweight) = reduceTupleNum(i)
      var rest = avgNumPerPart - curweight
      var j = 0
      while(j < reAssignKeyNum){
        val (key, weights,hashPart) = reAssignKeys(j)
        if(weights > 0 && weights < rest){
          rest = rest - weights
          //add [key, new partition] to strategy
          strategy(hashPart)(key) = part
          reAssignKeys(j) = (key, 0, hashPart)
        }
        j = j + 1
      }
      reduceTupleNum(i) = (part, avgNumPerPart - rest)
    }
    //asc
    reduceTupleNum = reduceTupleNum.sortBy(_._2)(reduceordering)
    //des
    reAssignKeys = reAssignKeys.filter(_._2 != 0).sortBy(_._2)(skewordering)

    var i = 0
    for((key, weights, hashPart) <- reAssignKeys){
      val (part, curweight) = reduceTupleNum(i)
      //println(i + " get " + weights + " before " +curweight)
      reduceTupleNum(i)= (part, curweight + weights)
      strategy(hashPart)(key) = part
      i = i + 1
    }
    return strategy
  }
}

/**
 * A [[org.apache.spark.Partitioner]] that implements hash-based partitioning using
 * Java's `Object.hashCode`.
 *
 * Java arrays have hashCodes that are based on the arrays' identities rather than their contents,
 * so attempting to partition an RDD[Array[_]] or RDD[(Array[_], _)] using a HashPartitioner will
 * produce an unexpected or incorrect result.
 */
class HashPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

/**
 * A [[org.apache.spark.Partitioner]] that partitions sortable records by range into roughly
 * equal ranges. The ranges are determined by sampling the content of the RDD passed in.
 *
 * @note The actual number of partitions created by the RangePartitioner might not be the same
 * as the `partitions` parameter, in the case where the number of sampled records is less than
 * the value of `partitions`.
 */
class RangePartitioner[K : Ordering : ClassTag, V](
                                                    partitions: Int,
                                                    rdd: RDD[_ <: Product2[K, V]],
                                                    private var ascending: Boolean = true)
  extends Partitioner {

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")

  private var ordering = implicitly[Ordering[K]]

  // An array of upper bounds for the first (partitions - 1) partitions
  private var rangeBounds: Array[K] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      val sampleSize = math.min(20.0 * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.length).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        RangePartitioner.determineBounds(candidates, partitions)
      }
    }
  }

  def numPartitions: Int = rangeBounds.length + 1

  private var binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K]

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition-1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: RangePartitioner[_, _] =>
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
        out.writeObject(binarySearch)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[Array[K]])
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
        binarySearch = in.readObject().asInstanceOf[(Array[K], K) => Int]

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
          rangeBounds = ds.readObject[Array[K]]()
        }
    }
  }
}

private[spark] object RangePartitioner {
  /**
   * Sketches the input RDD via reservoir sampling on each partition.
   *
   * @param rdd the input RDD to sketch
   * @param sampleSizePerPartition max sample size per partition
   * @return (total number of items, an array of (partitionId, number of items, sample))
   */
  def sketch[K : ClassTag](
                            rdd: RDD[K],
                            sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])]) = {
    val shift = rdd.id
    // val classTagK = classTag[K] // to avoid serializing the entire partitioner object
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }.collect()
    val numItems = sketched.map(_._2).sum
    (numItems, sketched)
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
                                                candidates: ArrayBuffer[(K, Float)],
                                                partitions: Int): Array[K] = {
    val ordering = implicitly[Ordering[K]]
    val ordered = candidates.sortBy(_._1)
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight >= target) {
        // Skip duplicate values.

        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }
}
