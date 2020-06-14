package main.scala.operator

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions

/**
  * Created by yasaka on 2016/6/8.
  */
object SortByKeyOperator {

  def main(args: Array[String]) {
      
    val conf = new SparkConf().setAppName("SortByKeyOperator")
      .setMaster("local")
      
    val sc = new SparkContext(conf)

    val scoreList = Array(Tuple2(150,"xuruyun"),Tuple2(100,"liangyongqi"),Tuple2(90,"wangfei"))
    
    
    val score = sc.parallelize(scoreList)
    
    
    val results = score.sortByKey(false)
    
    results.foreach(println)
  }
}
