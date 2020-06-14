package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object DistinctOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("JoinOperator")
      .set("spark.rdd.compress", "true")
      
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(Array(1,2,1,11,6,6,3,2))
    
    /**
     * map  KV格式的RDD
     * (1,null)
     * (2,null)
     * (2,null)
     * 
     * 
     * groupByKey/reduceByKey算子，可以对key去重
     * 1 [null]
     * 2 [null,null]
     * 
     */
    rdd.map { (_,null) }.groupByKey().map(_._1).foreach { println }
    
    /**
     * distinct这个 就是这样组合的
     * rdd.distinct = rdd.map { (_,null) }.groupByKey().map(_._1).foreach { println }
     */
     val rdd1 = sc.makeRDD(Array(("A",1),("B",1),("A",2),("B",1)))
    
     val distinctRDD = rdd1.distinct
    
     distinctRDD.foreach(println)
     
     val mapValuesRDD = rdd1.mapValues { x => (x,1) }
     
     
  }
}