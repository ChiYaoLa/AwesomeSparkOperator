package main.scala.operator

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ListBuffer

/**
  * Created by root on 2016/6/13.
  */
object MapPartitionsOperator {

  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("MapPartitionsOperator").setMaster("local")
    val sc = new SparkContext(conf)

    val arr = Array("xurunyun","liangyongqi","wangfei")
    val rdd = sc.parallelize(arr)
    
    rdd.filter { x => {
      println("filter")
      true
    } }.mapPartitions(x => {
      println("map")
      val list = new ListBuffer[String]()
      /**
       * 将RDD中的数据写入到数据库中，绝大部分使用mapPartition算子来实现
       */
      //创建一个数据库连接
      while(x.hasNext){
        //拼接SQL语句
        list += x.next()+"==="
      }
      //执行SQL语句  批量插入
      list.iterator
    }).count()
   
  }
}
