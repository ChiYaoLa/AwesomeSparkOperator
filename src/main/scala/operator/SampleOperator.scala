package main.scala.operator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SampleOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AggregateOperator").setMaster("local")
    val sc = new SparkContext(conf)
    val list = Array(
        "Angelababy",
        "Angelababy",
        "Angelababy",
        "Angelababy",
        "Angelababy",
        "xuruyun",
        "baibaihe",
        "liutao",
        "xiaobao")
        
    val rdd = sc.parallelize(list, 2)
    
    println("rdd.partitions.size:" + rdd.partitions.size)
    
    val sampleRDD = rdd.sample(false, 0.5)    
    
    sampleRDD.foreach { println }
  }
}



