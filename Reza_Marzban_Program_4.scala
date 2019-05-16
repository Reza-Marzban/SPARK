
// Reza Marzban 
// Spark Stream processing

//Stream twitter data into Spark for 3 hours. Identify three topic keywords. As the data is streaming in, based on the 3 topic keywords, count the number of times the keywords appear in the tweets. Output the count for each keyword. Caution: Do not use common words.

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object Reza_Marzban_Program_4 {
 def main(args:Array[String]){
 val conf=new SparkConf().setAppName("Program Number 4")
 val sc=new SparkContext(conf)
 sc.setLogLevel("ERROR")
 //creates Spark Session
 val spark = SparkSession.builder().appName("Program Number 4").getOrCreate()
 //tweets folder address on HDFS server -  ignore files with .tmp extensions (Flume active files).
 val inputpath="hdfs://HDFS Tweets Folder address"
 //create a Schema
 val dataSchema= spark.read.format("json").option("inferScehma","true").option("mode","dropMalformed").load(inputpath).schema
 //define the streaming
 val streamingData= spark.readStream.format("json").schema(dataSchema).option("maxFilesPerTrigger",10).load(inputpath)
 // just get the tweets text
 val tweets = streamingData.select( streamingData("text"))
 //set the keywords
 val items =  List("spring","april","nature")
 //transform tweets into lower case and count occurrences of keywords
 val wordCount=tweets.withColumn("word", functions.explode(functions.split(functions.lower(functions.col("text")), " "))).groupBy("word").count().where(functions.col("word").isin(items:_*))
 //set number of partitions
 spark.conf.set("spark.sql.shuffle.partitions",5)
 //start streaming and show the result in console as soon as a new file arrives
 val countQuery=wordCount.writeStream.queryName("countKeywords").format("console").outputMode("complete").start()
 //to finish streaming after 3 hours automatically. (10800000 miliseconds)
 countQuery.awaitTermination(10800000)
 //Launch an interruption to exit
 countQuery.stop()
 }
}

