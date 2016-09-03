#
# Alex Fok: Sep 2016
# Example of elasticsearch index read access from Spark
# Tested with spark 2.1.6, elasticsearch-hadoop-2.4.0.jar, elasticsearch 2.3.1
#  ./bin/spark-submit --driver-class-path /path/to/elasticsearch-hadoop-2.4.0.jar \
#        /path/to/examples/el_spark.py.py <host> <index> <type>
#

from __future__ import print_function

import sys

from pyspark import SparkContext



"""
Create data in  elasticsearch first:
curl -XPUT http://localhost:9200/alextest/alexusers/1 -d '{"user" : "alex1", "displayname" : "Alex1","message" : "trying out Elastic Search1"}'
curl -XPUT http://localhost:9200/alextest/alexusers/2 -d '{"user" : "alex2", "displayname" : "Alex2","message" : "trying out Elastic Search "}'
Get it:
curl http://localhost:9200/alextest/alexusers/1
"""

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""
        Usage: es_spark.py <host> <index> <type>

        Run with example jar:
        ./bin/spark-submit --driver-class-path /path/to/elasticsearch-hadoop-2.4.0.jar \
        /path/to/examples/es_spark.py.py <host> <index> <type>
        Assumes you have some data in elastic already, on <host>, in <index> and <type>
        """, file=sys.stderr)
        exit(-1)

    es_host = sys.argv[1]
    es_index = sys.argv[2]
    es_type = sys.argv[3]
    es_resource=es_index + "/" + es_type
    print (es_resource)
    sc = SparkContext(appName="ElasticSearchInputFormat")
    conf={ "es.resource" : es_resource,
           "es.nodes" : es_host,
           "es.port" : "9200"}

    es_rdd = sc.newAPIHadoopRDD(
     inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
     keyClass="org.apache.hadoop.io.NullWritable",
     valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
     conf=conf)
    print(es_rdd.first())
    output = es_rdd.collect()
    for (k, v) in output:
        print((k, v))

    sc.stop()

 
