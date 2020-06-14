package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RandomSplitoperator {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("RandomSplitoperator")
     
      
      val v = "bjsxt"
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(1 to 10)
 
    
    val splitRDD = rdd.randomSplit(Array(0.1, 0.2, 0.3, 0.4))
    println("splitRDD.size:" + splitRDD.size)

  }
}