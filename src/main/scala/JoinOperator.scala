package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object JoinOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("JoinOperator")
      .set("spark.rdd.compress", "true")
      
    val sc = new SparkContext(conf)
    
    /**
     * 创建RDD的方式：
     * 	1、使用本地集合创建一个RDD   并行化
     * 	2、通过读本地或者HDFS上的文件
     */
    val rdd1 = sc.parallelize(Array(Tuple2("A",1),("B",2),("C",1),Tuple2("A",2)) , 2)
    val rdd2 = sc.makeRDD(Array(Tuple2("A",100),("B",99),("C",101),("D",88)), 3)
    
    
    rdd1.join(rdd2).foreach(println(_))
    
    rdd1.leftOuterJoin(rdd2).foreach(println)
    rdd1.rightOuterJoin(rdd2).foreach(println)
    
  }
}