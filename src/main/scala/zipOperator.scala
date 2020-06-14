package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer

object zipOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("JoinOperator")
      .set("spark.rdd.compress", "true")
      
    val sc = new SparkContext(conf)
    
    val rdd1 = sc.parallelize(1 to 10,2)   // 1 2 3 4 5 10
    
    val rdd2 = sc.makeRDD(2 to 11,2)// 1 2 3 4 ..9 
    
    /**
     * 1、分区数需要相同
     * 2、分区中的元素个数相等
     */
    val rdd3 = rdd1.zip(rdd2)
    /**
     * （1,2）
     * （2，3）
     * ....
     * (10,11)
     */
    rdd3.foreach(println)
    
    /**
     * zipWithIndex 将RDD变成KV格式的RDD   K：这个RDD的元素  V：这个元素在RDD中的索引
     */
    val zipWithIndexRdd = rdd2.zipWithIndex()
    zipWithIndexRdd.foreach(println)
    
    /**
     * 将非KV格式的RDD变成KV格式的RDD  k:这个RDD的元素     V：第一个分区的第一个元素0+1    1+1=2 
     * (2,1)
     * (3,2)
     */
    rdd2.mapPartitionsWithIndex((index,iterator)=>{
       val list = new ListBuffer[Int]()
        while (iterator.hasNext) {
          val v = iterator.next()
          println("partition Id:"+index +"\tvalue："+v)
          list.+=(v)
        }
       list.iterator
    }, false).count()
    
    
    val zipWithUnipeIdRDD = rdd2.zipWithUniqueId()
    zipWithUnipeIdRDD.foreach(println)
    
  }
}