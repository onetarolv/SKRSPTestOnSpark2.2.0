#!/bin/bash
spark-submit --master spark://master:7077  --class org.apache.spark.RealDataTest.joinRealTest ./sparkTestFor2.2.0.jar  hdfs://master:9000/user/stack/RealData/1d* hdfs://master:9000/user/stack/RealData/v* $1 1.0 $2 0.07 1e-4 100 1000000
# echo "365 2.0596"
