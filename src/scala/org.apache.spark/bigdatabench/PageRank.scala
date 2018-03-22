/**
 * This program is based on examples of spark-0.8.0-incubating
 * The original source file is: org.apache.spark.examples.SparkPageRank
 */

package org.apache.spark.bigdatabench

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 */
object PageRank {
  def main(args: Array[String]) {
//    if (args.length < 1) {
//      System.err.println("Usage: PageRank <file> <number_of_iterations> <save_path> [<slices>]")
//      System.exit(1)
//    }
    val starttime = System.currentTimeMillis()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val filePath = if(args.length > 0) args(0) else "E:\\testdata\\v2\\acctest\\pagerank\\Google_genGraph_20.txt"
    val iters = if(args.length > 1) args(1).toInt else 15

    val save_path = if(args.length > 2) args(2) else "E:\\testdata\\v2\\acctest\\pagerank\\test"
    val slices = if (args.length > 3) args(3).toInt else 8
    val samplingRate = if (args.length > 4) args(4) else "0.5"
    val times = if (args.length > 5) args(5) else "1"
    val partitioner = if (args.length > 6) args(6) else "skrsp"
    val tol = if (args.length > 7) args(7) else "1.1"

    /*val ctx = new SparkContext(args(0), "PageRank",
      SPARK_HOME, Seq(TARGET_JAR_BIGDATABENCH))*/

    val conf = new SparkConf().setAppName("BigDataBench PageRank")//.setMaster("local")
     .set("spark.default.parallelism", slices.toString)
      .set("spark.partitioner.skewdegree", tol)
     .set("spark.partitioner.class", partitioner)
      .set("spark.partitioner.samplerate", samplingRate)
      .set("spark.partitioner.fraction", times)
    val ctx = new SparkContext(conf)

    // load data
    val lines = ctx.textFile(filePath, slices)

    // directed edges: (from, (to1, to2, to3))

    val link = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct()

    val links = link.groupByKey().cache()

    println(links.count.toString + " links loaded.")
    // rank values are initialised with 1.0
    var ranks = links.mapValues(v => 1.0).persist(StorageLevel.MEMORY_AND_DISK)

    for (i <- 1 to iters) {
      // calculate contribution to desti-urls
      val contribs = links.join(ranks).values.flatMap {
        case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }.persist(StorageLevel.MEMORY_AND_DISK)
      // This may lead to points' miss if a page have no link-in
      // add all contribs together, then calculate new ranks
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    // show results
    //val output = ranks.collect()"
    //output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    println("Result saved to: " + save_path)
    ranks.saveAsTextFile(save_path)
    println("----(T_T)---\nApplication executime time = " + (System.currentTimeMillis() - starttime).toDouble/1000 +
      "\n------------(#`O')\n------(`0'#)")
    ctx.stop()
    System.exit(0)
  }
}

