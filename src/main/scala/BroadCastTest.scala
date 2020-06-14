package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object BroadCastTest {
   def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("AggregateOperator").setMaster("local[3]")
        val sc = new SparkContext(conf)
        val userNames = Array(
            "xuruyun",    
            "zhouzhilei",
            "panglei",
            "laochen"
        )
        
        val userNameRDD = sc.parallelize(userNames)
       
        val blackNameBroadcaste = sc.broadcast("xuruyun")
        
        val linesCountAccumulator = sc.accumulator(0)
        val filteredUserNameRDD = userNameRDD.filter {x =>{
        	linesCountAccumulator.add(1)
          !blackNameBroadcaste.value.equals(x)
        }}
        
        filteredUserNameRDD.collect().foreach(println)
        println(linesCountAccumulator.value)
        
   }
}