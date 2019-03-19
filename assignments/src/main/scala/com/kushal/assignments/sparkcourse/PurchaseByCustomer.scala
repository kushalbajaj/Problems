package com.kushal.assignments.sparkcourse

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object PurchaseByCustomer {
  def main(args: Array[String]): Unit = {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc= new SparkContext("local[*]","PurchaseByCustomer")
    
    val input=sc.textFile("../customer-orders.csv")    
    
    def parseLines(line:String)=
    {
    val fields=  line.split(",")
    val customerId=fields(0)
    val amount=fields(2)
    (customerId,amount.toFloat)
    }
    
    val totalCustomersExpenses=input.map(parseLines(_)).reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1)).sortByKey()
    
    val results=totalCustomersExpenses.collect()
    results.foreach(show(_))
    
    def show(tup:Tuple2[Float,String])={
      println(s"${tup._2} : ${tup._1}")
    }
    
    
  }
}