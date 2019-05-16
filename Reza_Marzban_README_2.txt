
Reza Marzban 
__________________________________________
Program 2. Write a Spark job using the cube function to get the the average annual petroleum consumption in barrels for fuelType1 for each make of vehicle.

How To Run Reza_Marzban_Program_2.scala:
	1- On the spark cluster copy Reza_Marzban_Program_2.scala on folder structure: /src/main/scala/Reza_Marzban_Program_2.scala
	2- Create a file named "build.sbt" with following content:
name := "Program Number 2"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++=Seq(
 "org.apache.spark" %% "spark-core" % "2.0.1" % "provided",
 "org.apache.spark" %% "spark-sql" % "2.0.1"
)

	3- Copy build.sbt on your root directory beside src folder.
	4- Enter this Command: sbt clean
	5- Enter this Command: sbt package
	6- Enter this Command: spark-submit --class Reza_Marzban_Program_2 ./target/scala-2.10/program-number-2_2.10-1.0.jar
	
The output would be printed out on the console!