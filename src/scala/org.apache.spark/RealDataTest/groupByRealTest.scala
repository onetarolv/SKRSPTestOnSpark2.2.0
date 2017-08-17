package org.apache.spark.RealDataTest

import org.apache.spark.MapOutputTrackerMaster
import org.apache.spark.sql.SparkSession

/**
 * Created by DELL_PC on 2017/5/5.
 */

class groupByRealTest(partitioner : String, parallelism: Int, filePath: String, skewDegree: Double, alpha: Double, sampleRate: Double, sampleSizePerPart: Int, times: Double){
  def startTest(): Unit ={
    val spark = SparkSession.builder().appName("groupByTestOn" + partitioner)
      .config("spark.partitioner.class",partitioner)
      .config("spark.default.parallelism",parallelism)
      .config("spark.partitioner.skewdegree", skewDegree)
      .config("spark.partitioner.samplerate", sampleRate)
      .config("spark.partitioner.samplesize.lowerbound", sampleSizePerPart)
      .config("spark.partitioner.shash.alpha", alpha)
      .config("spark.partitioner.samplesize.times", times)
      .getOrCreate()

    //[key, value] : [Int, Array[Byte]
    val fileRDD = spark.sparkContext.textFile(filePath).map(_.split(" ")).filter(line => line.length==3)
      .map(line => (line(1),(line(0), line(2))))
    val grouprdd = fileRDD.groupByKey()
    val res = grouprdd.count()
    val tracker = spark.sparkContext.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val shuffleNum = tracker.shuffleNum
    for(i <- 0 until shuffleNum) {
      tracker.computeReduceDistribution(i)
    }
    spark.stop()
  }

}

object groupByRealTest{
  def main(args: Array[String]): Unit = {
      val filepath = if(args.length > 0) args(0) else  ""
      val partitoner = if(args.length > 1) args(1) else "hash"
      val skewDegree = if(args.length > 2) args(2) else "1.2"
      val parallelism = if(args.length > 3) args(3) else "40"
      val alpha = if(args.length > 4) args(4) else "0.07"
      val sampleRate = if(args.length > 5) args(5) else "1e-4"
      val sampleSizePerPart = if(args.length > 6) args(6) else "1000"
      val times = if(args.length > 7) args(7) else "10.0000"
     val start = System.currentTimeMillis()
      val test = new groupByRealTest(partitoner, parallelism.toInt, filepath, skewDegree.toDouble, alpha.toDouble, sampleRate.toDouble,sampleSizePerPart.toInt, times.toDouble)
      test.startTest()
     println("total time:" + (System.currentTimeMillis()-start))
  }
}