
// Reza Marzban 
// Spark Batch processing

//Write a Spark job using SQL to find the total number of makes of vehicles for all years that run on different types of fuelType1.

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Reza_Marzban_Program_3 {
 def main(args:Array[String]){
 val conf=new SparkConf().setAppName("Program Number 3")
 val sc=new SparkContext(conf)
 sc.setLogLevel("ERROR")
 //creates Spark Session
 val spark = SparkSession.builder().appName("Program Number 3").getOrCreate()
 //input csv file address on HDFS server
 val inputpath="hdfs://hdfs folder address"
 //create a data frame with CSV input data
 val OriginalDataFrame= spark.read.format("csv").option("inferScehma","true").option("header","true").option("mode","dropMalformed").load(inputpath)
 // just get the make and fueltype1 columns
 val cleanedDataFrame = OriginalDataFrame.select( OriginalDataFrame("make"), OriginalDataFrame("fueltype1"))
 //create a sql table from spark dataframe
 cleanedDataFrame.createOrReplaceTempView("cleaned_Data")
 //create a sql to get the number of distinct makes of vehicle for each different type of fueltype1
 val result=spark.sql("""SELECT fueltype1,COUNT(DISTINCT make) AS Count_Distinct_Make FROM cleaned_Data GROUP BY fueltype1 ORDER BY Count_Distinct_Make DESC""")
 //show top 150 rows
 result.show(150,false)
 }
}