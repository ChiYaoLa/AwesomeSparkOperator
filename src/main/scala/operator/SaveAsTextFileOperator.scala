package main.scala.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2016/6/13.
  */
object SaveAsTextFileOperator {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SaveAsTextFileOperator")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val numberArray = Array(1,2,3,4,5)
    val numbers = sc.parallelize(numberArray)

    val doubledNumbers = numbers.map(_*2)
    doubledNumbers.saveAsTextFile("hdfs://spark001:9000/double_number_20160613.txt")
  }
}
