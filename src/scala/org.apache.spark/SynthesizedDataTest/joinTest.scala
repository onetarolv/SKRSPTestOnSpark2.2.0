package org.apache.spark.SynthesizedDataTest

import java.net.URI

import breeze.numerics.sqrt
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.MapOutputTrackerMaster
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
 * Created by DELL_PC on 2017/4/19.
 */
class joinTest(partitioner : String, parallelism: Int, filePath1: String, filePath2: String, skewDegree: Double, alpha: Double, sampleRate: Double, sampleSizePerPart: Int, times: Double){
  def startTest(): Unit ={
    val spark = SparkSession.builder().appName("joinByTestOn" + partitioner)
      .config("spark.partitioner.class",partitioner)
      .config("spark.default.parallelism",parallelism)
      .config("spark.partitioner.skewdegree", skewDegree)
      .config("spark.partitioner.samplerate", sampleRate)
      .config("spark.partitioner.samplesize.lowerbound", sampleSizePerPart)
      .config("spark.partitioner.shash.alpha", alpha)
      .config("spark.partitioner.samplesize.times", times)
      .getOrCreate()


    val fileRDD1 = spark.sparkContext.textFile(filePath1).flatMap(_.split("\r\n")).filter(_.nonEmpty) .map(line => {
      val splitline = line.split(",")
      val bytearr = splitline(1).split(" ").filter(_.nonEmpty).map(_.toByte)
      (splitline(0).toInt, bytearr)

    })
    val fileRDD2 = spark.sparkContext.textFile(filePath2).flatMap(_.split("\r\n")).filter(_.nonEmpty).map(line => {
      val splitline = line.split(",")
      val bytearr = splitline(1).split(" ").filter(_.nonEmpty).map(_.toByte)
      (splitline(0).toInt, bytearr)
    })
    val res = fileRDD1.join(fileRDD2).map(x => {
      val o1 = x._2._1.mkString(" ")
      val o2 = x._2._1.mkString(" ")
      (x._1,"("+o1+","+ o2 +")")
    })
    res.count()

    val tracker = spark.sparkContext.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val reduceArr = new ArrayBuffer[Array[Long]]()
    val shuffleNum = tracker.shuffleNum
    for(i <- 0 until shuffleNum) {
      reduceArr.append(tracker.computeReduceDistribution(i))
    }
    val reduceNum = reduceArr(0).length
    val reduceData = Array.fill[Long](reduceNum)(0)
    for(i <-0 until shuffleNum) {
      for(j <- 0 until reduceNum){
        reduceData(j) += reduceArr(i)(j)
      }
    }
    val sum = reduceData.sum
    val avg = sum/reduceNum
    val s2 = reduceData.map(x => {
      (x - avg) * (x - avg)
    }).sum.toDouble/reduceNum
    val sd = sqrt(s2)/avg
    println("============sd is " + sd + "===========")
    spark.stop()
  }
}

object joinTest{
  def main(args: Array[String]): Unit = {
    val filepath1 = if(args.length > 0) args(0) else ""
    val filepath2 = if(args.length > 1) args(1) else ""
    val partitoner = if(args.length > 2) args(2) else "rejhash"
    val skewDegree = if(args.length > 3) args(3) else "1.2"
    val parallelism = if(args.length > 4) args(4) else "40"
    val alpha = if(args.length > 5) args(5) else "0.05"
    val sampleRate = if(args.length > 6) args(6) else "1e-4"
    val sampleSizePerPart = if(args.length > 7) args(7) else "40"
    val times = if(args.length > 8) args(8) else "10.0"
    val test = new joinTest(partitoner, parallelism.toInt, filepath1, filepath2, skewDegree.toDouble, alpha.toDouble, sampleRate.toDouble,sampleSizePerPart.toInt, times.toDouble)
    test.startTest()
  }
}