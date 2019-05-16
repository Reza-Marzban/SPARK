
// Reza Marzban 
// Spark Batch processing

//Write a Spark job using the cube function to get the the average annual petroleum consumption in barrels for fuelType1 for each make of vehicle.

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.SparkSession

object Reza_Marzban_Program_2 {
 def main(args:Array[String]){
 val conf=new SparkConf().setAppName("Program Number 2")
 val sc=new SparkContext(conf)
 sc.setLogLevel("ERROR")
 //creates Spark Session
 val spark = SparkSession.builder().appName("Program Number 2").getOrCreate()
 //input csv file address on HDFS server
 val inputpath="hdfs://hdfs folder address"
 //create a data frame with CSV input data
 val OriginalDataFrame= spark.read.format("csv").option("inferScehma","true").option("header","true").option("mode","dropMalformed").load(inputpath)
 // just get barrels08 and make column and convert barrels08 to double type
 val cleanedDataFrame = OriginalDataFrame.select( OriginalDataFrame("barrels08").cast(DoubleType).as("barrels08"), OriginalDataFrame("make"))
 //get average of each make with cube function
 val result = cleanedDataFrame.cube("make").avg("barrels08").sort(desc("avg(barrels08)"))
 //show top 150 rows
 result.show(150,false)
 }
}



