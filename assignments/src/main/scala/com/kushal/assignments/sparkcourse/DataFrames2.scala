package com.kushal.assignments.sparkcourse

import org.apache.log4j._
import org.apache.spark.sql._
    
object DataFrames2 {
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)
  
  def mapper(line:String): Person = {
    val fields = line.split(',')  
    
    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/Temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    val lines = spark.sparkContext.textFile("../fakefriends.csv")
    val i=0
    val abc=lines.map(l=>(i+1,l)).filter(i=>i._1/2==0)
    val size=abc.count();
    val needed=250;
    val factor=size/50
//    abc.values.foreach(println(_))
//    lines.zipWithIndex().sortBy(f=>f._2).collect().foreach(f=>println(f._2))
    val xyz=lines.zipWithIndex().sortBy(f=>f._2).filter(f=>math.abs(f._2 %factor)==0).collect()
    
    

    xyz.foreach(x=>println(x._1))
//    val people = lines.map(mapper).toDS().cache()
    

    // There are lots of other ways to make a DataFrame.
    // For example, spark.read.json("json file path")
    // or sqlContext.table("Hive table name")
    
/*    println("Here is our inferred schema:")
    people.printSchema()
    
    println("Let's select the name column:")
    people.select("name").show()
    
    println("Filter out anyone over 21:")
    people.filter(people("age") < 21).show()
   
    println("Group by age:")
    people.groupBy("age").count().show()
    
    println("Make everyone 10 years older:")
    people.select(people("name"), people("age") + 10).show()
    */
    spark.stop()
  }
}