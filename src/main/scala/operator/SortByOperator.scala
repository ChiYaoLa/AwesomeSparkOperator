package main.scala.operator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SortByOperator {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName("SortByKeyOperator")
      .setMaster("local")
      
    val sc = new SparkContext(conf)
     
    val scoreList = Array(Tuple3(190,100,"xuruyun"),Tuple3(100,202,"liangyongqi"),Tuple3(90,111,"wangfei"))

    val score = sc.parallelize(scoreList)

    score.map(x=>())
  }
}