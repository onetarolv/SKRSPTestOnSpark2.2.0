/*
 * Sort workload for BigDataBench
 */
package org.apache.spark.bigdatabench

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object Sort {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: Sort <data_file> <save_file>" +
        " [<slices>]")
      System.exit(1)
    }
    val starttime = System.currentTimeMillis()

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val numReducers = if (args.length > 2) args(2).toInt else 8
    val samplingRate = if (args.length > 3) args(3) else "0.5"
    val times = if (args.length > 4) args(4) else "1"
    val partitioner = if (args.length > 5) args(5) else "skrsp"
    val conf = new SparkConf().setAppName(s"BigDataBench Sort_$partitioner")
      .set("spark.partitioner.class", partitioner)
      .set("spark.partitioner.samplerate", samplingRate)
      .set("spark.partitioner.fraction", times)
    val spark = new SparkContext(conf)

    val filename = args(0)
    val save_file = args(1)
    val lines = spark.textFile(filename, numReducers).flatMap(line => line.split(" "))
    val data_map = lines.map(line => {
      (line, 1)
    })
    val result = data_map.sortByKey(true,numReducers).map { line => line._1}
    result.saveAsTextFile(save_file)
    println(s"Application executime time of $partitioner = " + (System.currentTimeMillis() - starttime).toDouble/1000)

    spark.stop()
  }

}