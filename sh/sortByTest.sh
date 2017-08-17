#!/bin/bash
spark-submit --master spark://master:7077  --class org.apache.spark.RealDataTest.sortByTest ./sparkTestFor2.2.0.jar hdfs://master:9000/user/stack/RealData/fin_enwiki $1 1.2 $2 0.07 1e-4 20 1000000
