package org.apache.spark

import java.util.Random

import org.apache.spark.sql.SparkSession

/**
 * Created by DELL_PC on 2018/1/4.
 */
object SkewWordCount {
  def main(args: Array[String]) {
    //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)



    val numMappers = if (args.length > 0) args(0).toInt else 4
    val numKVPairs = if (args.length > 1) args(1).toDouble.toInt else 1000//100000
    val numReducers = if (args.length > 2) args(2).toInt else numMappers
    val samplingRate = if (args.length > 3) args(3).toDouble else 0.5
    val times = if (args.length > 4) args(4).toDouble else 1
    val partitioner = if (args.length > 5) args(5) else "skrsp"
    val mapSideCombine: Boolean = if (args.length > 6) args(6).toBoolean else false
    val numKeys = if(args.length > 7) args(7).toDouble.toInt else numKVPairs
    val ratio = if (args.length > 8) args(8).toInt else 5.0
    val optype = if (args.length > 9) args(9) else "null"
    val fraction = 1.0 min (times * (8.0 / numMappers))

    val spark = SparkSession
      .builder
      .appName(s"SkewedWordCount_${partitioner}_${optype}")
      .config("spark.partitioner.class", partitioner)
      .config("spark.partitioner.samplerate", samplingRate)
      .config("spark.partitioner.fraction", fraction)
      .config("spark.partitioner.class", partitioner)
      .getOrCreate()

    val pairs1 = spark.sparkContext.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random
      var result = new Array[(Int, Int)](numKVPairs)
      for (i <- 0 until numKVPairs) {
        if (ranGen.nextDouble < 1.0 / numReducers ) {

          val offset = ranGen.nextInt(numKeys) * numReducers
          // give ratio times higher chance of generating key 0 (for reducer 0)
          result(i) = (offset, 1)
        } else {
          // generate a key for one of the other reducers
          val offset = ranGen.nextInt((numKeys/ratio).toInt) * numReducers
          val key = 1 + ranGen.nextInt(numReducers-1) + offset
          result(i) = (key, 1)
        }
      }
      result
    }.cache


     pairs1.reduceByKey(_ + _).saveAsTextFile("/lvwei/skewwordcount_result")

    val tracker = spark.sparkContext.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val shuffleNum = tracker.shuffleNum
    val totalReaders = Array.fill(numReducers)(0.0)
    for(i <- 0 until shuffleNum) {
      val shuffleReaders = tracker.computeReduceDistribution(i)
      for(r <- 0 until numReducers) {
        totalReaders(r) += shuffleReaders(r)
      }
    }
    val avgReader = totalReaders.sum / numReducers
    val readerSdidd = math.sqrt((totalReaders.map(size => (size - avgReader) * (size - avgReader)).sum) / numReducers) / avgReader
    println("============ shuffle reader : fos = " + readerSdidd + "===============")


    spark.stop()
  }
}
