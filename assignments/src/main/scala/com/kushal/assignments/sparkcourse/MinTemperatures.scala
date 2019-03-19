package com.kushal.assignments.sparkcourse

import org.apache.log4j._
import org.apache.spark._

import scala.math.max

/** Find the minimum temperature by weather station */
object MinTemperatures {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(1)
    val entryType = fields(2)
//    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    val prcp=fields(3).toFloat
    (stationID, entryType, prcp)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")
    
    // Read each line of input data
    val lines = sc.textFile("../1800.csv")
    
    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)
    
    // Filter out all but TMIN entries
    val minTemps = parsedLines.filter(x => x._2 == "PRCP")
    
    // Convert to (stationID, temperature)
    val stationTemps = minTemps.map(x => (x._1, x._3.toInt))
    
    // Reduce by stationID retaining the minimum temperature found
/*    val minTempsByStation = stationTemps.reduceByKey( (x,y) => max(x,y)).max()(new Ordering[Tuple2[String,Int]]{
      override  def compare(x:(String,Int), y:(String,Int)):Int={
        Ordering[Float].compare(x._2, y._2)
      }
    
    })*/
    
        val minTempsByStation = stationTemps.reduceByKey( (x,y) => max(x,y)).sortBy(x=>x._2, false, 0).take(1).mkString
     println(s"max temp $minTempsByStation") 
    // Collect, format, and print the results
/*    val results = minTempsByStation.collect()
    
    for (result <- results.sorted) {
       val station = result._1
       val temp = result._2
       val formattedTemp = f"$temp%.2f F"
       println(s"$station minimum temperature: $formattedTemp") 
    }*/
      
  }
}