package main.scala.operator

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
  * Created by root on 2016/6/7.
  */
object GroupByKeyOperator {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GroupByKeyOperator").setMaster("local")
    val sc = new SparkContext(conf)

    val scoreList = Array(Tuple2("xuruyun", 150), Tuple2("liangyongqi",100),
      Tuple2("wangfei",100),Tuple2("wangfei",80))
    val scores = sc.parallelize(scoreList)
    /**
     * wangfei [100,80]
     * xuruyun [150]
     * liangyongqi [100]
     */
    val groupedScores = scores.groupByKey()
    
    
    groupedScores.foreach(score => {
      println(score._1)
      score._2.foreach(everyScore => println(everyScore))
      println("=========================")
    })
  }
}
