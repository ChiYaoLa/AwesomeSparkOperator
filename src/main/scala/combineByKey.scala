package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.CompactBuffer

object combineByKey {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf()
      .setMaster("local")
      .setAppName("JoinOperator")
      .set("spark.rdd.compress", "true")
      
    val sc = new SparkContext(conf)
     
    val rdd1 = sc.parallelize(Array(Tuple2("A",1),("B",2),("B",3),("B",4),("B",5),("C",1),Tuple2("A",2)) , 2)
     
    /**
     * def combineByKey[C](
     * 		createCombiner: Int => C,
							为每一个分区中每一个分组进行初始化，如何初始化的？
								将createCobiner函数作用在这个分组的第一个元素上
     *  	mergeValue: (C, Int) => C,
     *  			按照mergeValue的聚合逻辑对每一个分区中每一个分组进行聚合
     *   	mergeCombiners: (C, C) => C): RDD[(String, C)]
     *   			reduce端的大聚合
     */
      rdd1.mapPartitionsWithIndex((index,iterator)=>{
      println("partitionId:" + index)
      val list = new ListBuffer[Tuple2[String,Int]]
      while(iterator.hasNext){
        val t = iterator.next()
        list.+=(t)
        println(t)
      }
      list.iterator
    }, true).count()
    
    
    rdd1.combineByKey(x=>x+"_", (x:String,y:Int)=>x+"@"+y, (x:String,y:String)=>x+"$"+y).foreach(println) 
    /**
     * 如何统计key出现的次数
     */
    rdd1.reduceByKey((x:Int,y:Int)=>x+y)
    rdd1.combineByKey(x=>x, (x:Int,y:Int)=>x+y, (x:Int,y:Int)=>x+y).foreach(println)
    
    /**
     * 使用combineByKey 模拟一个groupByKey    
     */
    val combineByKeyRDD =  rdd1.combineByKey(x=>ListBuffer(x), (list:ListBuffer[Int],y:Int)=>list += y, (listx:ListBuffer[Int],listy:ListBuffer[Int])=>listx ++= listy)
    /**
     * 使用combineByKey 模拟一个reduceByKey(_+_)
     */
    combineByKeyRDD.foreach(println)
  }
}
