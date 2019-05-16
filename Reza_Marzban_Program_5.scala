
// Reza Marzban 
// Spark Stream processing

//Write a Spark trigger based on the processing time. Execute the streaming query of No. 4 above at regular 10 minute intervals.

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.streaming.ProcessingTime

object Reza_Marzban_Program_5 {
 def main(args:Array[String]){
 val conf=new SparkConf().setAppName("Program Number 5")
 val sc=new SparkContext(conf)
 sc.setLogLevel("ERROR")
 //creates Spark Session
 val spark = SparkSession.builder().appName("Program Number 5").getOrCreate()
 //tweets folder address on HDFS server -  ignore files with .tmp extensions (Flume active files).
 val inputpath="hdfs://HDFS Tweets Folder address"
 //create a Schema
 val dataSchema= spark.read.format("json").option("inferScehma","true").option("mode","dropMalformed").load(inputpath).schema
 //define the streaming
 val streamingData= spark.readStream.format("json").schema(dataSchema).option("maxFilesPerTrigger",25).load(inputpath)
 // just get the tweets text
 val tweets = streamingData.select( streamingData("text"))
 //set the keywords
 val items =  List("spring","april","nature")
 //transform tweets into lower case and count occurrences of keywords
 val wordCount=tweets.withColumn("word", functions.explode(functions.split(functions.lower(functions.col("text")), " "))).groupBy("word").count().where(functions.col("word").isin(items:_*))
 //set number of partitions
 spark.conf.set("spark.sql.shuffle.partitions",5)
 //start streaming and show the result in console each 10 minutes (600 seconds)
 val countQuery=wordCount.writeStream.queryName("countKeywords").trigger(ProcessingTime("600 seconds")).format("console").outputMode("complete").start()
 //to finish streaming after 3 hours automatically. (10800000 miliseconds)
 countQuery.awaitTermination(10800000)
 //Launch an interruption to exit
 countQuery.stop()
 }
}