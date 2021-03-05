/*
	This class demonstrates the real-time streaming capabilities for Kafka and Spark
	Invoke as follows:
	$ spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2" --class main.scala.StreamingTest.QueryPOCStream target/scala-2.12/main-scala-streamingtest_2.12-1.0.jar
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

object QueryPOCStream
{
	def main(args: Array[String]) 
	{
		val spark = SparkSession
						.builder
						.appName("QueryPOCStream")
						.getOrCreate()
		
		// Open a read stream for a single Kafka topic
		val fileStreamDf = spark
					.readStream
					.format("kafka")
					.option("kafka.bootstrap.servers", "localhost:9092")
					.option("subscribe", "KafkaStreamPOC")
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
		
		
		// Calls per minute query
		// For write syncs to work on aggregated data, watermark needs to be applied to define old data timestamp thresholds.
		
		val queryCallsPerMin = query
								.withWatermark("TimeWithMinutes", "10 minutes")
								.groupBy(col("TimeWithMinutes"), window(col("TimeWithMinutes"), "10 minutes", "5 minutes"))
								.count()
		
		// Calls per 5 minute query						
		val queryCallsPer5Min = query
									.withWatermark("TimeWithMinutes", "10 minutes")
									.groupBy(col("TimeWithHours"),col("RoundedMinutes"), window(col("TimeWithMinutes"), "10 minutes", "5 minutes"))
									.count()
		
		// Calls per minute and per HTTP Method query							
		val queryCallsPerMinHTTPMethod = query
											.withWatermark("TimeWithMinutes", "10 minutes")
											.groupBy(col("TimeWithMinutes"), col("type"), window(col("TimeWithMinutes"), "10 minutes", "5 minutes"))
											.count()
		
		// Calls per 5 minute and per HTTP Method query										
		val queryCallsPer5MinHTTPMethod = query
											.withWatermark("TimeWithMinutes", "10 minutes")
											.groupBy(col("TimeWithHours"),col("RoundedMinutes"), col("type"), window(col("TimeWithMinutes"), "10 minutes", "5 minutes"))
											.count()
	
		
		// Start writing to file sink as JSON
		// Issue - Only one write stream is able to write the data and rest are getting bypassed 
		// 				as awaitTermination() method is not able to work together for all dataframes involved in the write stream
		
		queryCallsPerMin.writeStream
            .format("json")
            .outputMode("append")
            .option("path", "/Test/data/CallsPerMin/")
        	.option("checkpointLocation", "/Users/debdba/Desktop/Temp/Test/Check/")
        	.trigger(Trigger.ProcessingTime("20 seconds"))
            .start()

        
        queryCallsPer5Min.writeStream
            .format("json")
            .outputMode("append")
            .option("path", "/Test/data/CallsPer5Min/")
        	.option("checkpointLocation", "/Users/debdba/Desktop/Temp/Test/Check/")
        	.trigger(Trigger.ProcessingTime("20 seconds"))
            .start()

            
        queryCallsPerMinHTTPMethod.writeStream
            .format("json")
            .outputMode("append")
            .option("path", "/Test/data/CallsPerMinHTTPMethod/")
        	.option("checkpointLocation", "/Users/debdba/Desktop/Temp/Test/Check/")
        	.trigger(Trigger.ProcessingTime("20 seconds"))
            .start()

		// This is the only write sync that is working
		queryCallsPer5MinHTTPMethod.writeStream
            .format("json")
            .outputMode("append")
            .option("path", "/Test/data/CallsPer5MinHTTPMethod/")
        	.option("checkpointLocation", "/Users/debdba/Desktop/Temp/Test/Check/")
        	.trigger(Trigger.ProcessingTime("20 seconds"))
            .start()
            .awaitTermination() 
            
		
		
		spark.stop()
		
		// spark-submit --class main.scala.poc.QueryPOCStream target/scala-2.12/main-scala-poc_2.12-1.0.jar /Users/debdba/Desktop/Temp/Data
		
	}
}
