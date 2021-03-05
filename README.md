# StreamingPOC
This repository is for testing streaming examples from Spark and Kafka


Here are the steps run on a Macbook. Kafka, Spark are installed using brew:


1. Start Zookeeper and Kafka

$ zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties


2. Create a Kafka Topic
$ kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic KafkaStreamPOC

3. Add data to the topic excluding the csv header. The gateway_logs.csv contains the logs data that has to be streamed from Kafka to Spark

$ kafka-console-producer --broker-list localhost:9092 --topic KafkaStreamPOC < /Test/data/gateway_logs.csv

4. Validate if the consumer is able to get the data. This is a debug only Step

$ kafka-console-consumer --bootstrap-server localhost:9092 --topic KafkaStreamPOC 


5. Add QueryPOCStream.scala, QueryPOC.scala and build.sbt to a new directory. Build the package from this directory using the scala sbt tool

$ mkdir /<New-Dir>
$ cd /<New-Dir> 
$ sbt clean package

6. To check the non-streaming example run the following from the same directory as in above step

$ spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2" --class main.scala.StreamingTest.QueryPOC target/scala-2.12/main-scala-streamingtest_2.12-1.0.jar

7. To check the streaming example run the following from the same directory as in above step

$ spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2" --class main.scala.StreamingTest.QueryPOCStream target/scala-2.12/main-scala-streamingtest_2.12-1.0.jar
