package main.scala.operator

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
  * Created by yasaka on 2016/6/7.
  */
object LineCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LineCount").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("test.txt")
    val pairs = lines.map(x => (x ,1))
    val results = pairs.reduceByKey(_+_)
    results.foreach(println(_))
  }
}
