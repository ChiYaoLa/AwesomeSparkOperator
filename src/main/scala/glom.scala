package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object glom {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      .setMaster("local")
      .setAppName("JoinOperator")
      .set("spark.rdd.compress", "true")
      
    val sc = new SparkContext(conf)
    
    val rdd1 = sc.parallelize(1 to 10,2)   // 1 2 3 4 5 10
    
    /**
     * rdd1有两个分区
     * 	partition0分区里面的所有元素封装到一个数组
     * 	partition2分区里面的所有元素封装到一个数组
     */
    val glomRDD = rdd1.glom()
    glomRDD.foreach { _.foreach { println } }
    println(glomRDD.count())
    
    /**
     * randomSplit 根据我们传入的权重    Array，将rdd1拆分成Array.size个RDD
     * 拆分后的RDD中元素数量 由权重来决定
     */
    
    rdd1.randomSplit(Array(0.1,0.2,0.3,0.4)).foreach { x=>println(x.count) }
    
    
    
    
  }
}