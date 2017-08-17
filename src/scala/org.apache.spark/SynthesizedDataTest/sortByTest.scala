package org.apache.spark.SynthesizedDataTest

import org.apache.spark.MapOutputTrackerMaster
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
 * Created by DELL_PC on 2017/4/19.
 */
class sortByTest(partitioner : String, parallelism: Int, filePath: String, skewDegree: Double, alpha: Double, sampleRate: Double, sampleSizePerPart: Int, times: Double){
  def startTest(): Unit ={
    val spark = SparkSession.builder().appName("sortByTestOn" + partitioner)
      .config("spark.partitioner.class",partitioner)
      .config("spark.default.parallelism",parallelism)
      .config("spark.partitioner.skewdegree", skewDegree)
      .config("spark.partitioner.samplerate", sampleRate)
      .config("spark.partitioner.samplesize.lowerbound", sampleSizePerPart)
      .config("spark.partitioner.shash.alpha", alpha)
      .config("spark.partitioner.samplesize.times", times)
      .getOrCreate()
    val fileRDD = spark.sparkContext.textFile(filePath).flatMap(_.split(" ")).filter(_.nonEmpty).map(x => (x, 1))

    val sortRDD = fileRDD.sortByKey()//sort by word string
    sortRDD.count()
    val tracker = spark.sparkContext.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val shuffleNum = tracker.shuffleNum
    for(i <- 0 until shuffleNum) {
      tracker.computeReduceDistribution(i)
    }
  }
}

object sortByTest{
  def main(args: Array[String]): Unit = {
    val filepath = if(args.length > 0) args(0) else ""
    val partitoner = if(args.length > 1) args(1) else "rejrange"
    val skewDegree = if(args.length > 2) args(2) else "1.2"
    val parallelism = if(args.length > 3) args(3) else "20"
    val alpha = if(args.length > 4) args(4) else "0.05"
    val sampleRate = if(args.length > 5) args(5) else "1e-2"
    val sampleSizePerPart = if(args.length > 6) args(6) else "100"
    val times = if(args.length > 7) args(7) else "10000.0"
    val test = new sortByTest(partitoner, parallelism.toInt, filepath, skewDegree.toDouble, alpha.toDouble, sampleRate.toDouble,sampleSizePerPart.toInt, times.toDouble)
    test.startTest()
  }
}