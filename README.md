es_spark

Elastic Search access from Spark

Example of elasticsearch index read access from Spark
Tested with spark 2.1.6, elasticsearch-hadoop-2.4.0.jar, elasticsearch 2.3.1
./bin/spark-submit --driver-class-path /path/to/elasticsearch-hadoop-2.4.0.jar \
      /path/to/examples/el_spark.py.py <host> <index> <type>

Create data in  elasticsearch first:
curl -XPUT http://localhost:9200/alextest/alexusers/1 -d '{"user" : "alex1", "displayname" : "Alex1","message" : "trying out Elastic Search1"}'
curl -XPUT http://localhost:9200/alextest/alexusers/2 -d '{"user" : "alex2", "displayname" : "Alex2","message" : "trying out Elastic Search "}'
Get it:
curl http://localhost:9200/alextest/alexusers/1
