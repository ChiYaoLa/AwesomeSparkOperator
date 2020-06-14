package main.scala.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2016/6/7.
  */
object FilterOperator {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FilterOperator").setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = Array(1,2,3,4,5)
    val numberRDD = sc.parallelize(numbers)

    val filteredRDD = numberRDD.filter(_ % 2 == 0)
    filteredRDD.foreach(println)
  }
  
}
