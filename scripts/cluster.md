#flume启动命令：
bin/flume-ng agent \
--conf conf --name logger \
--conf-file conf/streaming_project.conf \
-Dflume.root.logger=DEBUG,console

#kafka启动命令：
bin/kafka-server-start.sh -daemon config/server.properties &

bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 1 --partitions 1 --topic Streaming

bin/kafka-console-consumer.sh --zookeeper master:2181 --topic Streaming


2、创建topic
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1  --topic Streaming
./bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 1 --partitions 1  --topic Streaming

6、查看topic情况
kafka-topics.sh --describe --zookeeper localhost:2181 --topic Streaming
./bin/kafka-topics.sh --describe --zookeeper master:2181 --topic Streaming

3、发送消息
【已验证OK】
kafka-console-producer.sh --topic Streaming --bootstrap-server localhost:9092
./bin/kafka-console-producer.sh --topic Streaming --bootstrap-server master:9092

#Spark提交命令：
bin/spark-shell --master local[2] --jars \
/opt/modules/spark-2.2.0-bin-2.6.0-cdh5.7.0/externallibs/spark-streaming-kafka-0-8_2.11-2.2.0.jar, \
/opt/modules/spark-2.2.0-bin-2.6.0-cdh5.7.0/externallibs/kafka_2.10-0.9.0.0.jar, \
/opt/modules/spark-2.2.0-bin-2.6.0-cdh5.7.0/externallibs/kafka-clients-0.9.0.0.jar, \
/opt/modules/spark-2.2.0-bin-2.6.0-cdh5.7.0/externallibs/zkclient-0.7.jar, \
/opt/modules/spark-2.2.0-bin-2.6.0-cdh5.7.0/externallibs/metrics-core-3.1.2.jar

#Spark源码编译命令：
./dev/make-distribution.sh --tgz \
-Phadoop-2.6 -Dhadoop-version=2.6.0-cdh5.7.0 \
-Pyarn \
-Phive-2.3.0 -Phive-thriftserver
