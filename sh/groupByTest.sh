#!/bin/bash
spark-submit --master spark://master:7077  --class org.apache.spark.RealDataTest.groupByRealTest ./sparkTestFor2.2.0.jar hdfs://master:9000/user/stack/RealData/1d* $1 1.0 $2 0.07 1e-2 20 1000000

