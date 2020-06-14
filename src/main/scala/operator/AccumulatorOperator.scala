package main.scala.operator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 注意点：
 * 	在Driver端定义，在Excutor端操作，不能再Executor端读取
 */
object AccumulatorOperator {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName("AccumulatorOperator").setMaster("local")
    val sc = new SparkContext(conf)

     val rdd = sc.textFile("F:\\JAVA\\JavaWorkSpace\\testmaven\\src\\main\\resources\\cs")
     
      val count = sc.accumulator(0)
    
     rdd.foreach { x => {
       count.add(1)
//       println(count.value)
     } }
      
     println(count.value)
     
     sc.stop()
    
    
    
  }
}