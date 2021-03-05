/*
	This class reads a Kafka topic every time it is executed and is an example of the non-streaming approach
	Invoke as follows:
	$ spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2" --class main.scala.StreamingTest.QueryPOC target/scala-2.12/main-scala-streamingtest_2.12-1.0.jar
*/


package main.scala.StreamingTest

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._


// import org.apache.spark.sql.Dataset
// import org.apache.spark.sql.Row
// import org.apache.spark.sql.streaming.OutputMode
// import org.apache.spark.sql.streaming.StreamingQuery

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
// import org.apache.spark.sql.Column

// import sqlContext.implicits._

import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}

object QueryPOC
{
	def main(args: Array[String]) 
	{
		val spark = SparkSession
						.builder
						.appName("QueryPOC")
						.getOrCreate()
		
		// Connect to a single Kafka topic
		val fileStreamDf = spark
					.read
					.format("kafka")
					.option("kafka.bootstrap.servers", "localhost:9092")
					.option("subscribe", "KafkaStreamPOC")
					//.option("startingOffsets", "earliest")
					.load()
		
		// Split the value field into csv columns
		val splitCSVDf = fileStreamDf
							.select(col("value").cast("string"))
							.alias("csv")
							.select("csv.*")

		// Assign datatypes to each field 
		val intervalDf = splitCSVDf
							.selectExpr("split(value,',')[0] as time","split(value,',')[1] as type", "split(value,',')[2] as incoming_url","split(value,',')[3] as microservice")

	
		
		
		// Do the transformations within the dataframe itself. 
		val query = intervalDf
						.withColumn("TimeWithMinutes", date_trunc("minute", substring_index(col("time"), ".", 1)))
						.withColumn("TimeWithHours", date_trunc("hour", substring_index(col("time"), ".", 1)))
						.withColumn("RoundedMinutes", date_format(date_trunc("minute", substring_index(col("time"), ".", 1)), "mm") - (date_format(date_trunc("minute", substring_index(col("time"), ".", 1)), "mm") % 5))
			
		// interval2.printSchema()
		
		// query.printSchema()
		
		query.createOrReplaceTempView("gatewayTable")
		
		// Debug
		// query.printSchema()
		
		// Debug
		// spark.sql("select count(1) from gatewayTable").show(false);
		
		// Calls per minute query
		val queryCallsPerMin = spark.sql("select TimeWithMinutes, count(1) as Counts from gatewayTable where microservice='API-Gateway' group by 1 order by 1")

		// Calls per 5 minute query		
		val queryCallsPer5Min = spark.sql("select TimeWithHours, RoundedMinutes, count(1) as Counts from gatewayTable where microservice='API-Gateway' group by 1, 2 order by 1, 2")

		// Calls per minute and per HTTP Method query	
		val queryCallsPerMinHTTPMethod = spark.sql("select TimeWithMinutes, type, count(1) as Counts from gatewayTable where microservice='API-Gateway' group by 1, 2 order by 1, 2")

		// Calls per 5 minute and per HTTP Method query	
		val queryCallsPer5MinHTTPMethod = spark.sql("select TimeWithHours, RoundedMinutes, type, count(1) as Counts from gatewayTable where microservice='API-Gateway' group by 1, 2, 3 order by 1,2 ")


		// Write to CSV files
		queryCallsPerMin.coalesce(1).write.mode("overwrite").option("header", "true").csv("/Test/data/CallsPerMin")	
		queryCallsPer5Min.coalesce(1).write.mode("overwrite").option("header", "true").csv("/Test/data/CallsPer5Min")	
		queryCallsPerMinHTTPMethod.coalesce(1).write.mode("overwrite").option("header", "true").csv("/Test/data/CallsPerMinHTTPMethod")	
		queryCallsPer5MinHTTPMethod.coalesce(1).write.mode("overwrite").option("header", "true").csv("/Test/data/CallsPer5MinHTTPMethod")	

            
		
		spark.stop()
		
		// spark-submit --class main.scala.poc.QueryPOCStream target/scala-2.12/main-scala-poc_2.12-1.0.jar /Users/debdba/Desktop/Temp/Data
		
	}
}
