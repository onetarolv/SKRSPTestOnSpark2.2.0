package org.apache.spark

/**
 * Created by DELL_PC on 2018/1/9.
 */
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable

object JoinTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val filePath1 = if(args.length >0 ) args(0) else "E:\\testdata\\v2\\acctest\\Join\\10l_1.0s_10p_1000words_25r_Join"
//    println("Float Value > Long Value = " + (Float.MaxValue > Long.MaxValue))
    val skrspstep_rate = if(args.length >1 ) args(1).toDouble else 0.5
    val skrspstep_size = if(args.length >2 ) args(2).toInt else 200
    val partitioner = if(args.length >3 ) args(3) else "libra"
    val times = if (args.length > 4) args(4).toDouble else 1
    val numReducers = if (args.length > 5) args(5).toInt else 16
    val savePath = if (args.length > 6) args(6) else "/lvwei/jointest_result"
    val filePath2 = if(args.length >7 ) args(7) else "E:\\testdata\\v2\\acctest\\Join\\10l_1.0s_10p_1000words_25r_Join"
    val optype = if (args.length > 8) args(8) else "join"

    val fraction = 1.0 min (times * (4.0 / 10))
    val conf = new SparkConf().setAppName(s"jointest_" + partitioner+ "_" + optype)
      .set("spark.partitioner.samplerate", skrspstep_rate.toString)
      .set("spark.partitioner.samplesize.lowerbound", skrspstep_size.toString)
      .set("spark.partitioner.fraction", fraction.toString)
      .set("spark.partitioner.class", partitioner)
    val sc = new SparkContext(conf)
    /*
    val rdd1 = sc.textFile(filePath1).filter(_.nonEmpty).flatMap(_.split("\\s+")).filter(_.nonEmpty).map(x => (x, 1))
    rdd1.reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey().foreach(println)

    val rdd2 = sc.textFile(filePath2).filter(_.nonEmpty).flatMap(_.split("\\s+")).filter(_.nonEmpty).map(x => (x, 1))
     */
    val rdd1 = sc.textFile(filePath1).filter(_.nonEmpty).map(line => (line.split(",")(0).toLong, line.split(",")(1)))
    val rdd2 = sc.textFile(filePath2).filter(_.nonEmpty).map(line => (line.split(",")(0).toLong, line.split(",")(1)))

    val part = if(partitioner.contains("skrsp"))
      new KRHPALL(numReducers, false, optype, rdd1, rdd2)
    else if(partitioner.contains("range"))
      new RangePartitioner(numReducers, rdd1)
    else if(partitioner.contains("libra"))
      new LIBRA(numReducers, rdd1, false, "join")
    else
      new HashPartitioner(numReducers)

//    rdd1.map(x => (x._1, 1)).reduceByKey(_ + _).map(x => (part.getPartition(x._1), x._2)).reduceByKey(_ + _).sortByKey().collect().foreach(println)
//    println("==============")
//    rdd1.map(x => (x._1, 1)).reduceByKey(_ + _, numReducers).map(x=>(x._1,("id=" + new HashPartitioner(numReducers).getPartition(x._1),x._2))).collect().sortBy(_._1).foreach(println)

    val joinrdd = rdd1.join(rdd2, part)

    val partitionSizes = joinrdd.mapPartitions(iter => {
      var count = 0L
      while(iter.hasNext){
        count = count + 1
        iter.next()
      }
      Iterator(count)
    }).collect()

    val tracker = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val shuffleNum = tracker.shuffleNum
    val totalReaders = Array.fill(numReducers)(0.0)
    for(i <- 0 until shuffleNum) {
      val shuffleReaders = tracker.computeReduceDistribution(i)
     for(r <- 0 until numReducers) {
       totalReaders(r) += shuffleReaders(r)
     }
    }
   // println("10400000002 hash id = " + part.getPartition("10400000002".toLong))
    val avgReader = totalReaders.sum / numReducers
    val readerSdidd = math.sqrt((totalReaders.map(size => (size - avgReader) * (size - avgReader)).sum) / numReducers) / avgReader
    println("============ shuffle reader : sdidd = " + readerSdidd + "===============")

    val avgPartitions = partitionSizes.sum / numReducers
    val partitionSdidd = math.sqrt((partitionSizes.map(size => (size - avgPartitions) * (size - avgPartitions)).sum) / numReducers) / avgPartitions
    println("============after shuffle partition : sdidd = " + partitionSdidd + "===============")
    var p = 0
    partitionSizes.foreach(x => {
      println("size of partition "+ p + " = " + x)
      p = p + 1
    })

    val skrspSizes = totalReaders.zip(partitionSizes).map(len => (len._1 + len._2))
    val avgSkrsp = skrspSizes.sum / numReducers
    val skrspSdidd = math.sqrt((skrspSizes.map(size => (size - avgSkrsp) * (size - avgSkrsp)).sum) / numReducers) / avgSkrsp
    println("==============skrsp sdidd = " + skrspSdidd + "===============")
    //println("size length: " + sizes.length)
     val hashres =  joinrdd.mapPartitionsWithIndex((idx, iter) => {
        val hashMap = new mutable.HashMap[Long, Int]
        while(iter.hasNext){
          val key = iter.next()
          hashMap(key._1) = hashMap.getOrElse(key._1, 0) + 1
        }
        Iterator((idx, hashMap.size))
      }).collect()
      hashres.foreach(x => println(x))
//
//    val skrsppart = new KRHPForJoin(numReducers, false, "join", rdd1, rdd2)
//    val skrspres = rdd1.join(rdd2, skrsppart).map(x => x._1).mapPartitionsWithIndex((idx, iter) => {
//      val hashMap = new mutable.HashMap[Long, Int]
//      while(iter.hasNext){
//        val key = iter.next()
//        hashMap(key) = hashMap.getOrElse(key, 0) + 1
//      }
//      Iterator((idx, (hashMap.size, hashMap.keySet.mkString(" "))))
//    }).collect()
//    skrspres.foreach(x => println(x))
//    println("equal ? " + hashres.sameElements(skrspres))

    sc.stop
  }
}
