package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object BroadcastOperator {
  def main(args: Array[String]): Unit = {
    
     val conf = new SparkConf().setAppName("MapOperator").setMaster("local")
    val sc = new SparkContext(conf)
   
    val array = Array("xuruyun","liangyongqi","zhangsan","lisi")
    
    val nameRdd = sc.parallelize(array)
    val blackRDD = sc.makeRDD(Array("xuruyun","liangyongqi"))
    
    val blackList = List("xuruyun","liangyongqi")
    /**
     * 
     */
    val blackListBroadcast = sc.broadcast(blackList)
    nameRdd.filter { x => {
     !blackListBroadcast.value.contains(x)
      
    } }.foreach { println}
    
    
    /**
     * 如何将一个RDD广播出去
     * 	实际上广播出去的时候RDD中的数据
     * 如何在Driver端拿到一个RDD的数据 collect
     */
    
  }
}