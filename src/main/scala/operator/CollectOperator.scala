package main.scala.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2016/6/13.
  */
object CollectOperator {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CollectOperator")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val numberArray = Array(1,2,3,4,5)
    val numbers = sc.parallelize(numberArray)

    val doubleNumbers = numbers.map(_*2)
    val doubleNumbersArray = doubleNumbers.collect()
    for(num <- doubleNumbersArray){
      println(num)
    }
  }
}
