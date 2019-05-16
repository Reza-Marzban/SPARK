
Reza Marzban
__________________________________________
Program 4. Stream twitter data into Spark for 3 hours. Identify three topic keywords. As the data is streaming in, based on the 3 topic keywords, count the number of times the keywords appear in the tweets. Output the count for each keyword. Caution: Do not use common words. 

How To Run Reza_Marzban_Program_4.scala:
	1- On the spark cluster copy Reza_Marzban_Program_4.scala on folder structure: /src/main/scala/Reza_Marzban_Program_4.scala
	2- Create a file named "build.sbt" with following content:
name := "Program Number 4"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++=Seq(
 "org.apache.spark" %% "spark-core" % "2.0.1" % "provided",
 "org.apache.spark" %% "spark-sql" % "2.0.1",
 "org.apache.spark" % "spark-streaming_2.10" % "2.1.0"
)

	3- Copy build.sbt on your root directory beside src folder.
	4- Enter this Command: sbt clean
	5- Enter this Command: sbt package
	6- Enter this Command*: spark-submit --class Reza_Marzban_Program_4 ./target/scala-2.10/program-number-4_2.10-1.0.jar
	
The output would be printed out on the console!


________________________________________________________________________________________________________________________________________________________________________________________________________
if you want to start the Flume streaming again, run the following configuration file on Apache Flume:

TwitterAgent.sources = Twitter
TwitterAgent.channels = MemChannel
TwitterAgent.sinks = HDFS
TwitterAgent.sources.Twitter.type = com.cloudera.flume.source.TwitterSource
TwitterAgent.sources.Twitter.channels = MemChannel
TwitterAgent.sources.Twitter.consumerKey = MbOZITAXtTJIMyWTTtlgHf4Hy
TwitterAgent.sources.Twitter.consumerSecret = BaHQyCovaP5mxnQkYLwaibAIqLMZiqrvNFwyWhBnyXtCRD39tb
TwitterAgent.sources.Twitter.accessToken = 1000618234167603201-JdA0jtvtrg7sfpWlKGbgMpZnx8JW0x
TwitterAgent.sources.Twitter.accessTokenSecret = deZXEErKDslc9ORT9O8OdGvX4inVHXxQeNZwGoAi5hm4M
TwitterAgent.sources.Twitter.keywords = spring, april, nature
TwitterAgent.sinks.HDFS.channel = MemChannel
TwitterAgent.sinks.HDFS.type = hdfs
TwitterAgent.sinks.HDFS.hdfs.path = hdfs://hdfs tweets address
TwitterAgent.sinks.HDFS.hdfs.fileType = DataStream
TwitterAgent.sinks.HDFS.hdfs.writeFormat = Text
TwitterAgent.sinks.HDFS.hdfs.batchSize = 100
TwitterAgent.sinks.HDFS.hdfs.rollSize = 0
TwtterAgent.sinks.HDFS.hdfs.rollCount = 0
TwitterAgent.channels.MemChannel.type = memory
TwitterAgent.channels.MemChannel.capacity = 10000
TwitterAgent.channels.MemChannel.transactionCapacity = 10000