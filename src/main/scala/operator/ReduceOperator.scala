package main.scala.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2016/6/13.
  */
object ReduceOperator {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ReduceOperator")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val numberArray = Array(1,2,3,4,5)
    val numbers = sc.parallelize(numberArray)
    val sum = numbers.reduce(_+_)
    println(sum)
  }
}
