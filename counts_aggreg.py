#
# Alex Fok: Sep 2016
# Example of counters aggregation over Spark
# Counters input file format (11.txt):
# key value
# aa 11
# bb 22
# Tested with spark 2.1.6
#  ./bin/spark-submit counts_aggreg.py 11.txt
#
# The script keeps its state between invocations in file “~/sum.txt
# You can see it this way:
# more ~/sum.txt/part-00000

import os
import shutil
import sys
from pyspark import SparkContext, SparkConf

###########################################################
################
## Helpers
# Converts RDD element into blank separated line to be written to file
def toCSVLine(data):
  return ' '.join(str(d) for d in data)

def touchFile(filename):
  try:
    os.utime(filename, None)
  except:
    open(filename, 'a').close()    

def silentRemove(filename):
  try:
    if os.path.exists(filename):
	  if os.path.isfile(filename):
        os.remove(filename)
      else:
        shutil.rmtree(filename)
  except:
    print("Failed to delete file, trying to continue execution")

## Main flow
if __name__ == "__main__":
################
## Main

	if len(sys.argv) != 2:
	  print("Usage: counts_aggreg input_file")
	  exit(-1)
	infile=sys.argv[1]

	# create Spark context with Spark configuration
	conf = SparkConf().setAppName("Counts Aggregations")
	sc = SparkContext(conf=conf)

	## Calculate new_counters from file sys.argv[1]
	new_counters = sc.textFile(infile).map(lambda x: (x.split(" ")[0], int(x.split(" ")[1]))).reduceByKey(lambda a, b: a + b)
	print new_counters.collect()

	################
	## touch sum.txt
	touchFile("sum.txt")
	
	## Load old_counters from file "sum.txt"
	old_counters = sc.textFile("sum.txt").map(lambda x: (x.split(" ")[0], int(x.split(" ")[1]))).reduceByKey(lambda a, b: a + b)
	print old_counters.collect()

	################
	## Aggregate new_counters with old_counters - in agg_counts
	agg_counts = old_counters.union(new_counters).reduceByKey(lambda a, b: a + b)
	# agg_counts = agg_counts.union(new_counters).reduceByKey(lambda a, b: a + b)
	## Cache agg_counts
	agg_counts.persist()
	## print agg_counts
	print agg_counts.collect()

	# Delete old "sum.txt" file
	silentRemove("sum.txt")

	# Save agg_counts back to "sum.txt"
	agg_counts.coalesce(1).map(toCSVLine).saveAsTextFile("sum.txt")

#########################################################
