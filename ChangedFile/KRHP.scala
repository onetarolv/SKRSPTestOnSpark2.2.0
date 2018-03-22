package org.apache.spark

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math._
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.hashing._

/**
 * Created by DELL_PC on 2017/12/13.
 */
class KRHP[K: ClassTag, V](
   partitions: Int,
   mapSideCombine: Boolean,
   optype: String,
   self: RDD[_],//RDD[_ <: Product2[K, V]],
   others: RDD[_]*) extends Partitioner() with Logging{
  override def numPartitions: Int =  partitions

  val flag = classOf[Product2[K,V]].isAssignableFrom(self.elementClassTag.runtimeClass)
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    val hashIdx = getHashPartition(k)
    if(strategy.contains(hashIdx)){
      strategy(hashIdx).getOrElse(k, hashIdx)
    }else{
      hashIdx
    }
  }

  val rdds = (Seq(self) ++ others)

  private val SAMPLE_RATE = self.conf.getDouble("spark.partitioner.samplerate", 0.2)
  private val SKEW_TOLERANCE = self.conf.getDouble("spark.partitioner.skewdegree", 1.2)
  private val SAMPLE_SIZE_LOWER_BOUND = self.conf.getInt("spark.partitioner.samplesize.lowerbound", 1000)
  private val SAMPLE_ALPHA = self.conf.getDouble("spark.partitioner.shash.alpha", 0.07)
  private val SAMPLE_TIMES = self.conf.getDouble("spark.partitioner.samplesize.times", 10000000.0)
  private val NODES_NUMBER = self.conf.getInt("spark.nodenumber", 8)
  private val SAMPLE_FRACTION = 1.0 min ((self.conf.getDouble("spark.partitioner.fraction", 1.0)) * (NODES_NUMBER.toDouble/self.getNumPartitions))
  private val PARTITIONER = self.conf.get("spark.partitioner.class", "skrsp")

  // val mapSideCombine: Boolean = false
  private var strategy: mutable.HashMap[Int, mutable.HashMap[K, Int]] = {
    if (partitions <= 1 || !flag) {
      mutable.HashMap.empty
    } else {
      val joinStr = Array("join", "rightOuterJoin", "leftOuterJoin", "subtractByKey","fullOuterJoin", "joinnull", "rightOuterJoinnull", "leftOuterJoinnull")
      val (keyToWeight, totalWeight) = {
        if(joinStr.contains(optype)) {
          generateJoinStrategy()
        }else {
          generateOrdinaryStrategy()
        }
      }
      val starttime = System.currentTimeMillis()

      val strategy = if(PARTITIONER.contains("bf")) {
        reAssignKeysByBestFit(keyToWeight, partitions, SKEW_TOLERANCE, totalWeight)
      } else {
        reAssignKeyToPartition(keyToWeight, partitions, SKEW_TOLERANCE, totalWeight)
      }
      println("Execute Time Of Strategy Generation = " + (System.currentTimeMillis() - starttime))
      strategy
    }
  }

  def generateJoinStrategy(): (Array[(K, Float)], Double) = {
    var starttime = System.currentTimeMillis()
    var totalWeight = 0.0
    val keyToNum = new mutable.HashMap[K, (Float, Float)]()
    val pairrdds = rdds.map(_.asInstanceOf[RDD[_ <: Product2[K, _]]])
    var i = 0
    val clusters = new mutable.HashMap[K, Array[Double]]()
    for (rdd <- pairrdds) {
      //val rdd = self.asInstanceOf[RDD[_ <: Product2[K, _]]]
      val sampleSize = math.min(SAMPLE_SIZE_LOWER_BOUND * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      // the sample size is the lower boundary, the real smaple size will be caculated later.
      val sampleSizeLowerBound = math.ceil(sampleSize / rdd.getNumPartitions).toInt
      println("sampleSizeLowerBound = " + sampleSizeLowerBound)
      println("fraction = " + SAMPLE_FRACTION)
      val starttime = System.currentTimeMillis()
      //computeRDDToKeyWeight(rdd, sampleSizeLowerBound, (sampleSizeLowerBound * SAMPLE_TIMES).toInt, SAMPLE_ALPHA, SAMPLE_RATE))
      val sketched = {
        if (rdd.conf.get("spark.partitioner.class", "skrsp").equals("skrsp-r"))
          KRHP.sketchReservoir(rdd.map(_._1), sampleSizeLowerBound, SAMPLE_FRACTION)
        //KRHPTest.sketchReservoir(rdd.map(_._1), sampleSizeLowerBound, (sampleSizeLowerBound * SAMPLE_TIMES).toInt, SAMPLE_ALPHA, SAMPLE_RATE, SAMPLE_FRACTION)
        else
          KRHP.sketchRejection(rdd.map(_._1), sampleSizeLowerBound, (sampleSizeLowerBound * SAMPLE_TIMES).toInt, SAMPLE_ALPHA, SAMPLE_RATE, SAMPLE_FRACTION)
      }
      if (sketched.isEmpty) {
        println("sample is empty!!")
        mutable.HashMap.empty
      } else {
        var totaltime = 0.0
        if (mapSideCombine) {
          sketched.foreach { case (id, weight, sample, time) =>
            println(s"sampling time for ${id} partition = " + time)
            println(s"sampling size for ${id} partition = " + sample.size)

            totaltime += time
            for ((k, freq) <- sample) {
              //val kweight = weight * freq
              val v1v2 = keyToNum.getOrElse(k, (0f, 0f))
              keyToNum(k) = if (i == 0) (v1v2._1 + 1, v1v2._2) else (v1v2._1, v1v2._2)
              totalWeight += 1
            }
          }
        } else {
          sketched.foreach { case (id, weight, sample, time) =>
            println(s"sampling time for ${id} partition = " + time)
            println(s"sampling size for ${id} partition = " + sample.size)
            totaltime += time

            for ((k, freq) <- sample) {
              val kweight = weight * freq * (1f/SAMPLE_FRACTION.toFloat)
              val v1v2 = keyToNum.getOrElse(k, (0f, 0f))
              keyToNum(k) = if (i == 0) (v1v2._1 + kweight, v1v2._2) else (v1v2._1, v1v2._2 + kweight)
            }
          }
        }
        i = i + 1
        println("average time for one partition = " + totaltime / sketched.length)
      }
    }
    println("Execute Time Of Sampling = " + (System.currentTimeMillis() - starttime))
    starttime = System.currentTimeMillis()
    def calculateWeights(optype: String, kv1v2: (K, (Float, Float))): (K, Float) = {
      val (key, (v1, v2)) = if(i == 2) kv1v2 else (kv1v2._1, (kv1v2._2._1, kv1v2._2._1))
      val newValue = {
        if (optype.equals("join")) {
          v1 * v2 + v1 + v2
        } else if (optype.equals("leftOuterJoin")) {
          if (v2 != 0f)
            v1 * v2 + v1 + v2
          else
            v1 + v1 + v2
        } else if (optype.equals("rightOuterJoin")){
          if (v1 != 0f)
            v1 * v2 + v1 + v2
          else
            v2 + v1 + v2
        } else if (optype.equals("fullOuterJoin")){
          if(v1 == 0)
            v1 + v2 + v2
          else if (v2 == 0)
            v1 + v1 + v2
          else
            v1 * v2 + v2 + v1
        } else if (optype.equals("subtractByKey")) {
            if(v2 != 0)
              v1 + v2
            else
              v1 + v2 + v1
        } else {
          v1 + v2
        }
      }
      totalWeight += newValue
      (key, newValue)
    }
    val keyToWeight = keyToNum.toArray.map(kv1v2 => calculateWeights(optype, kv1v2))
    (keyToWeight, totalWeight)
  }

  def generateReduceByKeyStrategy(): (Array[(K, Float)], Double) = {
    var totalWeight = 0.0
    val keyToNum = new mutable.HashMap[K, Float]()
    val pairrdds = rdds.map(_.asInstanceOf[RDD[_ <: Product2[K, _]]])
    for (rdd <- pairrdds) {
      //val rdd = self.asInstanceOf[RDD[_ <: Product2[K, _]]]
      val sampleSize = math.min(SAMPLE_SIZE_LOWER_BOUND * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      // the sample size is the lower boundary, the real smaple size will be caculated later.
      val sampleSizeLowerBound = math.ceil(sampleSize / rdd.getNumPartitions).toInt
      val starttime = System.currentTimeMillis()
      val sketched = {
        if (rdd.conf.get("spark.partitioner.class", "skrsp").equals("skrsp-r"))
          KRHP.sketchReservoirForReduceByKey(rdd.map(_._1), sampleSizeLowerBound, SAMPLE_FRACTION)
        else
          KRHP.sketchRejectionForReduceByKey(rdd.map(_._1), sampleSizeLowerBound, (sampleSizeLowerBound * SAMPLE_TIMES).toInt, SAMPLE_ALPHA, SAMPLE_RATE, SAMPLE_FRACTION)
      }
      println("Execute Time Of Sampling = " + (System.currentTimeMillis() - starttime))

      if (sketched.isEmpty) {
        (mutable.HashMap.empty, Array.empty)
      } else {
        sketched.foreach { case sample =>
          for ((k, freq) <- sample) {
            keyToNum(k) = keyToNum.getOrElse(k, 0f) + 1f
            totalWeight += 1.0
          }
        }
      }
    }

    def calculateWeights(optype: String, kv: (K, Float)): (K, Float) = {
      val (key, value) = kv
      if(optype.equals("reduceByKey"))
        (key, value + 1f)
      else
        kv
    }

    val keyToWeight = keyToNum.toArray.map(kv => calculateWeights(optype, kv))
    (keyToWeight, totalWeight)
  }

  def generateOrdinaryStrategy(): (Array[(K, Float)], Double) = {
    var totalWeight = 0.0
    val keyToNum = new mutable.HashMap[K, Float]()
    val pairrdds = rdds.map(_.asInstanceOf[RDD[_ <: Product2[K, _]]])
    for (rdd <- pairrdds) {
      //val rdd = self.asInstanceOf[RDD[_ <: Product2[K, _]]]
      val sampleSize = math.min(SAMPLE_SIZE_LOWER_BOUND * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      // the sample size is the lower boundary, the real smaple size will be caculated later.
      val sampleSizeLowerBound = math.ceil(sampleSize / rdd.getNumPartitions).toInt
      val starttime = System.currentTimeMillis()
      //computeRDDToKeyWeight(rdd, sampleSizeLowerBound, (sampleSizeLowerBound * SAMPLE_TIMES).toInt, SAMPLE_ALPHA, SAMPLE_RATE))
      val sketched = {
        if (rdd.conf.get("spark.partitioner.class", "skrsp").equals("skrsp-r"))
          KRHP.sketchReservoir(rdd.map(_._1), sampleSizeLowerBound, SAMPLE_FRACTION)
        //KRHPTest.sketchReservoir(rdd.map(_._1), sampleSizeLowerBound, (sampleSizeLowerBound * SAMPLE_TIMES).toInt, SAMPLE_ALPHA, SAMPLE_RATE, SAMPLE_FRACTION)
        else
          KRHP.sketchRejection(rdd.map(_._1), sampleSizeLowerBound, (sampleSizeLowerBound * SAMPLE_TIMES).toInt, SAMPLE_ALPHA, SAMPLE_RATE, SAMPLE_FRACTION)
      }
      println("Execute Time Of Sampling = " + (System.currentTimeMillis() - starttime))

      if (sketched.isEmpty) {
        (mutable.HashMap.empty, Array.empty)
      } else {
        if(mapSideCombine){
          sketched.foreach { case (idx, weight, sample, time) =>
            for ((k, freq) <- sample) {
              keyToNum(k) = keyToNum.getOrElse(k, 0f) + 1f
              totalWeight += 1
            }
          }
        }else {
          sketched.foreach { case (idx, weight, sample, time) =>
            for ((k, freq) <- sample) {
              val kweight = weight * freq * (1f / SAMPLE_FRACTION.toFloat)
              keyToNum(k) = keyToNum.getOrElse(k, 0f) + kweight
              totalWeight += kweight
            }
          }
        }
      }
    }
    def calculateWeight(optype: String, kv: (K, Float)): (K, Float) = {
      val (key, value) = kv
      if (optype.contains("combine") || optype.contains("groupByKey")) {
        (key, value + 1)
      }else {
        (key, value + value)
      }
    }
    val keyToWeight = keyToNum.toArray.map(kv => calculateWeight(optype, kv))
    (keyToWeight, totalWeight)
  }
  def getHashPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, partitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: KRHP[_, _] =>
      h.strategy.sameElements(strategy)
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions

  def reAssignKeyToPartition[K : ClassTag](
    keyToNumber: Array[(K, Float)],
    partitions: Int,
    skewDegree:Double,
    totalWeights: Double): mutable.HashMap[Int, mutable.HashMap[K, Int]] = {

    println("use First Fit !!")

    //val ordering = implicitly[Ordering[K]]
    // get the weights of keys
    val keyNumber = keyToNumber.length
    //      implicit val ordering = new Ordering[Double] {
    //        override def compare(x: Double, y: Double): Double = {
    //          y.compareTo(x)
    //        }
    //      }
    implicit val reduceordering = new Ordering[Double] {
      override def compare(a: Double, b: Double) = a.compare(b)
    }
    implicit val skewordering = new Ordering[Float] {
      override def compare(a: Float, b: Float) = b.compare(a)
    }

    //val sort = keyToNumber.sortBy(_._2)(skewordering)

    //val keyToTotalWeight = keyToNumber.sortBy(_._2)
    //val keyToPartition = new mutable.HashMap[K, Int]()
    val step = totalWeights / partitions

    var reducerWeights = new Array[(Int, Double)](partitions)
    val partToKey = new Array[ArrayBuffer[(K, Float)]](partitions)

    for (i <- 0 until partitions) {
      reducerWeights(i) = (i, 0)
      partToKey(i) = new ArrayBuffer[(K, Float)]()
    }
    val reAssignParts = new ArrayBuffer[Int]()
    //use HashPartitioner, caculate tuples number of every partition

    for(i <- 0 until keyNumber){
      val (key, weights) = keyToNumber(i)
      val p = getHashPartition(key)
      partToKey(p).append((key, weights))
      reducerWeights(p) = (p, reducerWeights(p)._2 + weights)
    }
    //      implicit val reduceordering = new Ordering[Double] {
    //        override def compare(a: Double, b: Double) = a.compare(b)
    //      }
    //      implicit val skewordering = new Ordering[Double] {
    //        override def compare(a: Double, b: Double) = b.compare(a)
    //      }
    //get the average number for one partition
    val avgWeight = reducerWeights.map(_._2).sum/partitions
    val threshold = avgWeight * skewDegree
    var redisKeys = new ArrayBuffer[(K, Float)]()
    for(i <- 0 until partitions){
      val (part, curweights) = reducerWeights(i)
      if(curweights > threshold){
        // des by weights
        var rest = threshold
        val keys = partToKey(i).toArray.sortBy(_._2)(skewordering)
        for((key, weights) <- keys){
          if(rest >= weights || rest == threshold){
            rest = rest - weights
          }else{
            redisKeys.append((key,weights))
          }
        }
        reAssignParts.append(part)
        reducerWeights(i) = (part, threshold - rest)
      }
    }
    reducerWeights = reducerWeights.sortBy(_._2)(reduceordering)

    val reAssignKeyNum = redisKeys.length
    val strategy = new mutable.HashMap[Int, mutable.HashMap[K, Int]]()

    //fill every partitions using bigger tuple clusters, but every partitions is smaller than avgNumPerPart
    for(i <- 0 until partitions){
      val (part, curweight) = reducerWeights(i)
      var rest = avgWeight - curweight
      var j = 0
      while(rest> 0 && j < reAssignKeyNum){
        val (key, weights) = redisKeys(j)
        if(weights > 0 && weights <= rest){
          rest = rest - weights
          val hashIdx = getHashPartition(key)
          if(!strategy.contains(hashIdx)){
            strategy(hashIdx) = new mutable.HashMap[K, Int]
          }
          strategy(hashIdx)(key) = part
          redisKeys(j) = (key, 0)
        }
        j = j + 1
      }
      reducerWeights(i) = (part, avgWeight - rest)
    }
    //asc
    reducerWeights = reducerWeights.sortBy(_._2)(reduceordering)
    //des
    redisKeys = redisKeys.filter(_._2 != 0).sortBy(_._2)(skewordering)

    var i = 0
    for((key, weights) <- redisKeys){
      val (part, curweight) = reducerWeights(i)
      reducerWeights(i)= (part, curweight + weights)
      val hashIdx = getHashPartition(key)
      if(!strategy.contains(hashIdx)){
        strategy(hashIdx) = new mutable.HashMap[K, Int]
      }
      strategy(hashIdx)(key) = part
      i = i + 1
    }
    println("redis partition is:" + reAssignParts.mkString(","))
    println("redis key number is:" + reAssignKeyNum)

    strategy
  }

  def reAssignKeysByBestFit[K : ClassTag](
                                              clusterWeights: Array[(K, Float)],
                                              partitions: Int,
                                              skewDegree:Double,
                                              totalWeights: Double): mutable.HashMap[Int, mutable.HashMap[K, Int]] = {
      println("use Best Fit !!")
      val avgWeight = totalWeights/partitions
      val threshold = skewDegree * avgWeight
      var reducerWeights = new Array[(Int, Double)](partitions)
      val partToKey = new Array[ArrayBuffer[(K, Float)]](partitions)

      for (i <- 0 until partitions) {
        reducerWeights(i) = (i, 0)
        partToKey(i) = new ArrayBuffer[(K, Float)]()
      }
      val reAssignParts = new ArrayBuffer[Int]()
      val keyNumbers = clusterWeights.length
      //use HashPartitioner, caculate tuples number of every partition

      for(i <- 0 until keyNumbers){
        val (key, weights) = clusterWeights(i)
        val p = getHashPartition(key)
        partToKey(p).append((key, weights))
        reducerWeights(p) = (p, reducerWeights(p)._2 + weights)
      }
    //asc
    implicit val ascending = new Ordering[Double] {
      override def compare(a: Double, b: Double) = a.compare(b)
    }
    //des
    implicit val descending = new Ordering[Float] {
      override def compare(a: Float, b: Float) = b.compare(a)
    }
    //get the average number for one partition
    var redisKeys = new ArrayBuffer[(K, Float)]()
    for(i <- 0 until partitions){
      val (part, curweight) = reducerWeights(i)
      if(curweight > threshold){
        // des by weights
        var rest = threshold
        val keys = partToKey(i).toArray.sortBy(_._2)(descending)
        var ik = 0
        while(ik < keys.length){
          //        for((key, weights) <- keys){
          val (key, weight) = keys(ik)
          if(rest >= weight || ik == 0){
            rest = rest - weight
          }else{
            redisKeys.append((key,weight))
          }
          ik = ik + 1
        }
        reAssignParts.append(part)
        reducerWeights(i) = (part, rest - (threshold - avgWeight))
      }else {
        reducerWeights(i) = (part, avgWeight - curweight)
      }
    }
    //generate partition strategy
    val strategy = new mutable.HashMap[Int, mutable.HashMap[K, Int]]()
    val sortedRedisKeys = redisKeys.sortBy(_._2)(descending)
    for((key, weight) <- sortedRedisKeys){
      reducerWeights = reducerWeights.sortBy(_._2)(ascending)
      val hashIdx = getHashPartition(key)
      var d = 0
      while(d < partitions){
        val (idx, restWeight) = reducerWeights(d)
        if(restWeight >= weight || d == (partitions - 1)) {
          if(!strategy.contains(hashIdx)){
            strategy(hashIdx) = new mutable.HashMap[K, Int]
          }
          strategy(hashIdx)(key) = idx
          reducerWeights(d) = (idx, restWeight - weight)
          d = partitions
        }
        d = d + 1
      }
    }
    strategy
  }





  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>

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
        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag1 = ds.readObject[ClassTag[mutable.HashMap[Int, mutable.HashMap[K, Int]]]]()
          strategy = ds.readObject[mutable.HashMap[Int, mutable.HashMap[K, Int]]]()
        }
    }
  }
}

object KRHP{
  def sketchRejection[K : ClassTag](
                                     rdd: RDD[K],
                                     sampleSizeLowerBound: Int,
                                     sampleSizeUpperBound: Int,
                                     alpha: Double,
                                     sampleRate: Double,
                                     fraction: Double = 1.0):  Array[(Int, Float, mutable.HashMap[K,Long], Long)]= {
    //      val start = System.currentTimeMillis()
    val iterrdd = rdd.mapPartitionsWithIndex((id, iter) => Iterator((id, iter)))
    //val sizes = iterrdd.map(_._2.size)
    val sampleIdx = fraction * rdd.getNumPartitions
    val sampleRDD = {
      if (fraction == 1.0)
        iterrdd
      else
        iterrdd.filter(_._1 < sampleIdx)
    }
    val sketched = sampleRDD.zip(sampleRDD.map(_._2.size)).map(id_iter_size => {
      val starttime = System.currentTimeMillis()
      val id = id_iter_size._1._1
      val iter = id_iter_size._1._2
      val size = id_iter_size._2
      val (sample, weight, time) = rejectionSampleAndCount(
        iter, sampleSizeLowerBound, sampleSizeUpperBound, alpha, size, sampleRate)
      //println(s"Execute Time Of sample ${id} partition = " +  (System.currentTimeMillis() - starttime ))
      (id, weight, sample, time)
    }).collect()
    //      println("rejection time :" + (System.currentTimeMillis()-start))
    sketched
  }

  def sketchReservoir[K : ClassTag](
                                     rdd: RDD[K],
                                     sampleSizePerPartition: Int,
                                     fraction: Double = 1): Array[(Int, Float, mutable.HashMap[K,Long], Long)] = {

    val shift = rdd.id
    // val classTagK = classTag[K] // to avoid serializing the entire partitioner object
    val sampleRDD = {
      if (fraction == 1.0)
        rdd
      else
        rdd.sample(false, fraction)
    }
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, weight, time) = reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, weight, sample, time))
    }.collect()
    sketched
  }

  def reservoirSampleAndCount[T: ClassTag](
                                            input: Iterator[T],
                                            k: Int,
                                            seed: Long = Random.nextLong())
  : (mutable.HashMap[T, Long], Float, Long) = {
    val starttime = System.currentTimeMillis()
    val reservoir = new Array[T](k)

    // Put the first k elements in the reservoir.
    var i = 0
    while (i < k && input.hasNext) {
      val item = input.next()
      reservoir(i) = item
      i += 1
    }

    // If we have consumed all the elements, return them. Otherwise do the replacement.
    val (sample, len) = if (i < k) {
      // If input size < k, trim the array to return only an array of input size.
      val trimReservoir = new Array[T](i)
      System.arraycopy(reservoir, 0, trimReservoir, 0, i)
      (trimReservoir, i.toDouble)
    } else {
      // If input size > k, continue the sampling process.
      var l = i.toLong
      val rand = new XORShiftRandom(seed)
      while (input.hasNext) {
        val item = input.next()
        l += 1
        // There are k elements in the reservoir, and the l-th element has been
        // consumed. It should be chosen with probability k/l. The expression
        // below is a random long chosen uniformly from [0,l)
        val replacementIndex = (rand.nextDouble() * l).toLong
        if (replacementIndex < k) {
          reservoir(replacementIndex.toInt) = item
        }
      }
      (reservoir, l.toDouble)
    }
    val keyFreq = new mutable.HashMap[T, Long]()
    for(key <- sample) {
      keyFreq(key) = keyFreq.getOrElse(key, 0L) + 1L
    }
    (keyFreq, len.toFloat / k, System.currentTimeMillis() - starttime)
  }

  def v_prime(u: Double, n: Double): Double = {
    pow(u, 1.0/ n)
  }

  def rejectionSampleAndCount[T: ClassTag](
                                            input: Iterator[T],
                                            sampleSizeLowerBound: Int,
                                            sampleSizeUpperBound: Int,
                                            alpha: Double,
                                            len: Long,
                                            sampleRate: Double)
  : (mutable.HashMap[T, Long], Float, Long) = {
    val starttime = System.currentTimeMillis()
    val keyFreq = new mutable.HashMap[T, Long]()
    val iter = input
    //val len = seq.length //numbers of intput datapoints
    var n: Double = {
      val size = (sampleRate * len).toInt.toDouble
      if(size < sampleSizeLowerBound){
        sampleSizeLowerBound
      }else if(size > sampleSizeUpperBound){
        sampleSizeUpperBound
      }else{
        size
      }
    }
    var N = len.toDouble
    val weight = len.toFloat / n.toFloat
    println("sampling size =" + n + "; sampling rate = "  + 1/weight)
    val threshold = (1/alpha) * n
    var i = 0
    val rng = new Random()
    var step = 0
    val sample = new ArrayBuffer[T]()
    val quan1 = N - n + 1
    val quan2 = quan1 / N
    var u = rng.nextDouble()
    var vprime = v_prime(u, n)
    while(n > 1 && iter.hasNext && threshold < N){
      var flag = false
      var x = N * (1.0 - vprime)
      var step = x.toInt
      while(step >= N - n - 1){
        u = rng.nextDouble()
        vprime = v_prime(u, n)
        x = N * (1.0 - v_prime(u, n))
        step = x.toInt
        u = rng.nextDouble()
      }
      //make sure that step < N - n - 1
      var y = rng.nextDouble()/ quan2
      val LHS = v_prime(y, n - 1)
      val RHS = ((quan1 - step)/quan1) * (N / (N - x))

      if(LHS <= RHS){
        flag = true
      }else{
        var (bottom, limit) = {
          if(n - 1 > step){
            (N - n, N - step)
          }else{
            (N - step - 1, quan1)
          }
        }
        var top = N - 1

        while(top >= limit){
          y = y * top / bottom
          bottom = bottom - 1
          top = top - 1
        }
        val LFS = v_prime(y, n - 1)
        val RFS = N / (N - x)
        if(LFS <= RFS){
          flag = true
          vprime = v_prime(rng.nextDouble(), n - 1)
        }else{
          vprime = v_prime(rng.nextDouble(), n)
        }
        //
      }
      if(flag){
        addSampleItem(step, iter, keyFreq)
        N = N - 1 - step
        n = n - 1
        i = i + step + 1
      }
    }
    if(n > 1){
      //call algorith A
      stepSimpleSmaple(N, n, i, iter, keyFreq)
    }else{
      step = (N * vprime).toInt
      addSampleItem(step, iter, keyFreq)
    }

    (keyFreq, weight, System.currentTimeMillis() - starttime)
  }
  def addSampleItem[T: ClassTag](step: Int, iter: Iterator[T], keyFreq: mutable.HashMap[T, Long]): Unit ={
    var stmp = step
    while(iter.hasNext && stmp > 0){
      stmp = stmp - 1
      iter.next()
    }
    val key = iter.next
    keyFreq(key) = keyFreq.getOrElse(key, 0L) + 1L
    //sample.append(iter.next())
  }
  def stepSimpleSmaple[T: ClassTag](valN: Double, valn: Double, vali: Int, iter : Iterator[T], keyFreq: mutable.HashMap[T, Long]): Unit ={
    var N = valN
    var n = valn
    var i = vali
    val rng = new Random()
    var top = N - n
    var flag = true
    var step = 0
    while(n >= 2 && flag){
      top = N - n
      val v = rng.nextDouble()
      step = 0
      var quot = top / N
      while(quot > v){
        step = step + 1
        top = top - 1
        quot = quot * top / N
      }
      var stmp = step
      while(iter.hasNext && stmp > 0){
        stmp = stmp - 1
        iter.next()
      }
      if(iter.hasNext){
        val key = iter.next
        keyFreq(key) = keyFreq.getOrElse(key, 0L) + 1
        N = N - 1 - step
        n = n - 1
      }else{
        flag = false
      }
      //println(step + ":"  + item)
    }
    if(n == 1){
      step = (N * rng.nextDouble()).toInt
      addSampleItem(step, iter, keyFreq)
    }
  }

  //============================================
  def sketchReservoirForReduceByKey[K : ClassTag](
                                                   rdd: RDD[K],
                                                   sampleSizePerPartition: Int,
                                                   fraction: Double = 1): Array[Set[(K,Int)]] = {

    val shift = rdd.id
    // val classTagK = classTag[K] // to avoid serializing the entire partitioner object
    val sampleRDD = {
      if (fraction == 1.0)
        rdd
      else
        rdd.sample(false, fraction)
    }
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val sample = reservoirSampleAndCountForReduceByKey(
        iter, sampleSizePerPartition, seed)
      Iterator(sample.map(key => (key, 1)))
    }.collect()
    sketched
  }
  def reservoirSampleAndCountForReduceByKey[T: ClassTag](
                                                          input: Iterator[T],
                                                          k: Int,
                                                          seed: Long = Random.nextLong())
  : Set[T] = {
    val reservoir = new Array[T](k)

    // Put the first k elements in the reservoir.
    var i = 0
    while (i < k && input.hasNext) {
      val item = input.next()
      reservoir(i) = item
      i += 1
    }
    // If we have consumed all the elements, return them. Otherwise do the replacement.
    val (sample, len) = if (i < k) {
      // If input size < k, trim the array to return only an array of input size.
      val trimReservoir = new Array[T](i)
      System.arraycopy(reservoir, 0, trimReservoir, 0, i)
      (trimReservoir, i.toDouble)
    } else {
      var l = i.toLong
      val rand = new XORShiftRandom(seed)
      while (input.hasNext) {
        val item = input.next()
        l += 1
        val replacementIndex = (rand.nextDouble() * l).toLong
        if (replacementIndex < k) {
          reservoir(replacementIndex.toInt) = item
        }
      }
      (reservoir, l.toDouble)
    }
    val keySet = reservoir.toSet
    keySet
  }

  def sketchRejectionForReduceByKey[K : ClassTag](
                                                   rdd: RDD[K],
                                                   sampleSizeLowerBound: Int,
                                                   sampleSizeUpperBound: Int,
                                                   alpha: Double,
                                                   sampleRate: Double,
                                                   fraction: Double = 1.0):  Array[Set[(K,Int)]]= {
    //      val start = System.currentTimeMillis()
    val iterrdd = rdd.mapPartitionsWithIndex((id, iter) => Iterator((id, iter)))
    val sampleRDD = {
      if (fraction == 1.0)
        iterrdd
      else
        iterrdd.sample(false, fraction)
    }
    //val sizes = iterrdd.map(_._2.size)

    val sketched = sampleRDD.zip(sampleRDD.map(_._2.size)).map(id_iter_size => {
      val input = id_iter_size._1._2
      val size = id_iter_size._2
      val sample = rejectionSampleAndCountForReduceByKey(
        input, sampleSizeLowerBound, sampleSizeUpperBound, alpha, size, sampleRate)
      sample.map(key => (key, 1)).toSet
    }).collect()
    println("sample size :" + sketched.map(_.size).sum)
    sketched
  }
  def rejectionSampleAndCountForReduceByKey[T: ClassTag](
                                                          input: Iterator[T],
                                                          sampleSizeLowerBound: Int,
                                                          sampleSizeUpperBound: Int,
                                                          alpha: Double,
                                                          len: Long,
                                                          sampleRate: Double)
  : (mutable.Set[T]) = {
    //      val starttime = System.currentTimeMillis()
    val iter = input
    var keySet = mutable.Set.empty[T]

    //val len = seq.length //numbers of intput datapoints
    var n: Double = {
      val size = (sampleRate * len).toInt.toDouble
      if(size < sampleSizeLowerBound){
        sampleSizeLowerBound
      }else if(size > sampleSizeUpperBound){
        sampleSizeUpperBound
      }else{
        size
      }
    }
    //println("len=" + len)
    var N = len.toDouble
    val weight = len / n
    println("change sampling size =" + n + "; sampling rate = "  + 1/weight)
    val threshold = (1/alpha) * n
    var i = 0
    val rng = new Random()
    var step = 0
    val sample = new ArrayBuffer[T]()
    var quan1 = N - n + 1
    var quan2 = quan1 / N
    var u = rng.nextDouble()
    var vprime = v_prime(u, n)
    while(n > 1 && iter.hasNext && threshold < N){
      var flag = false
      var x = N * (1.0 - vprime)
      var step = x.toInt
      while(step >= N - n - 1){
        u = rng.nextDouble()
        vprime = v_prime(u, n)
        x = N * (1.0 - v_prime(u, n))
        step = x.toInt
        u = rng.nextDouble()
      }
      //make sure that step < N - n - 1
      var y = rng.nextDouble()/ quan2
      val LHS = v_prime(y, n - 1)
      val RHS = ((quan1 - step)/quan1) * (N / (N - x))

      if(LHS <= RHS){
        flag = true
      }else{
        var (bottom, limit) = {
          if(n - 1 > step){
            (N - n, N - step)
          }else{
            (N - step - 1, quan1)
          }
        }
        var top = N - 1

        while(top >= limit){
          y = y * top / bottom
          bottom = bottom - 1
          top = top - 1
        }
        val LFS = v_prime(y, n - 1)
        val RFS = N / (N - x)
        if(LFS <= RFS){
          flag = true
          vprime = v_prime(rng.nextDouble(), n - 1)
        }else{
          vprime = v_prime(rng.nextDouble(), n)
        }
        //
      }
      if(flag){
        addSampleItemForReduceByKey(step, iter, keySet)
        N = N - 1 - step
        n = n - 1
        i = i + step + 1
      }
    }
    if(n > 1){
      //call algorith A
      stepSimpleSmapleForReduceByKey(N, n, i, iter, keySet)
    }else{
      step = (N * vprime).toInt
      addSampleItemForReduceByKey(step, iter, keySet)
    }
    //println("Execute Time Of total : " +  (System.currentTimeMillis() - starttime ))
    keySet
  }
  def addSampleItemForReduceByKey[T: ClassTag](step: Int, iter: Iterator[T], keySet: mutable.Set[T]): Unit ={
    var stmp = step
    while(iter.hasNext && stmp > 0){
      stmp = stmp - 1
      iter.next()
    }
    val key = iter.next
    keySet += key
    //sample.append(iter.next())
  }
  def stepSimpleSmapleForReduceByKey[T: ClassTag](valN: Double, valn: Double, vali: Int, iter : Iterator[T], keySet: mutable.Set[T]): Unit ={
    var N = valN
    var n = valn
    var i = vali
    val rng = new Random()
    var top = N - n
    var flag = true
    var step = 0
    while(n >= 2 && flag){
      top = N - n
      val v = rng.nextDouble()
      step = 0
      var quot = top / N
      while(quot > v){
        step = step + 1
        top = top - 1
        quot = quot * top / N
      }
      var stmp = step
      while(iter.hasNext && stmp > 0){
        stmp = stmp - 1
        iter.next()
      }
      if(iter.hasNext){
        val key = iter.next
        keySet += key
        N = N - 1 - step
        n = n - 1
      }else{
        flag = false
      }
      //println(step + ":"  + item)
    }
    if(n == 1){
      step = (N * rng.nextDouble()).toInt
      addSampleItemForReduceByKey(step, iter, keySet)
    }
  }
}

//object skrspUtils{
//  def computeKeyToTotalWeight[K: Ordering : ClassTag](
//     keyToNumber: Array[(K, Float)]): (Array[(K, Float)], Double) = {
//    val ordering = implicitly[Ordering[K]]
//    val keyToTotalNums = new ArrayBuffer[(K, Float)]()
//    val ordered = keyToNumber.sortBy(_._1)
//    //
//    var preKey = ordered(0)._1
//    var totalKeyWeght: Double = 0
//    var oneKeyWeight: Float = 0
//    for((key, weight) <- ordered){
//      if( !key.equals(preKey)){
//        keyToTotalNums += ((preKey, oneKeyWeight))
//        totalKeyWeght = totalKeyWeght + oneKeyWeight
//        oneKeyWeight = weight
//      }else{
//        oneKeyWeight = oneKeyWeight + weight
//        //keyToTotalNums += (totalKeyNum + keyToTotalNums.last._2)
//      }
//      preKey = key
//    }
//    if(oneKeyWeight !=0 ){
//      totalKeyWeght = totalKeyWeght + oneKeyWeight
//      keyToTotalNums += ((preKey, oneKeyWeight))
//    }
//    (keyToTotalNums.toArray, totalKeyWeght)
//  }
//}
