package main.scala.operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
  * Created by yasaka on 2016/6/2.
  */
object WordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    // RDD 分布式弹性数据集
    val text = sc.textFile("hs_err_pid5848.log")
    // flatMap = map + flat
    val words = text.flatMap(_.split(" "))
    
    
    val pairs = words.map((_,1))
    // reduceByKey = reduce + groupByKey
    var results = pairs.reduceByKey(_+_)
    
    val tmpRDD = results.map(x=>{(x._2,x._1)})
    val sortRdd = tmpRDD.sortByKey(false)
    
    results = sortRdd.map(x=>{(x._2,x._1)})
    
    results.foreach(println(_))
  }
}
