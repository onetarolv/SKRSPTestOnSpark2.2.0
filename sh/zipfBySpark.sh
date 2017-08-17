#!/bin/bash
spark-submit --master spark://master:7077  --class GenerateBySpark ./ZipfBySpark.jar 1.0 1 1 5000
