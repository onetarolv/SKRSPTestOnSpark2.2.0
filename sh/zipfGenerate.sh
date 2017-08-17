#!/bin/bash
echo "<<------generate zipf data -------->>"
java -jar Zipf.jar $1 57813 /home/stack/storage/partitiontest/randomSkewData 
