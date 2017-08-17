#!/bin/bash
spark-submit --master spark://master:7077  --class GenerateJoinData ./ZipfBySpark.jar $1 500 10 1000
