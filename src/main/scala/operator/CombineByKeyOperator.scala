package main.scala.operator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer

object CombineByKeyOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("CombineByKeyOperator")
    val sc = new SparkContext(conf)
    
    val rdd1 = sc.makeRDD(Array(
          ("A",1),
          ("A",3),
          ("A",4),
          ("A",5),
          ("A",2),
          ("B",1),
          ("B",2),
          ("C",1)
        ),3)
    rdd1.mapPartitionsWithIndex((index,iterator)=>{
      println(index)
      val list = new ListBuffer[Tuple2[String,Int]]()
      while (iterator.hasNext) {
        val log = iterator.next()
        println(log)
        list += log
      }
      list.iterator
    }).count()
    
    
    rdd1.combineByKey(
        (v:Int)=>v+"_",
        (c:String,v:Int) => {c + "@" + v} ,
        (c1:String,c2:String) => c1+"$"+c2,
        4
    ).collect().foreach(println)
  }
}