package main.scala.operator

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
  * Created by yasaka on 2016/6/8.
  */
object JoinOperator {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JoinOperator")
      .setMaster("local")
     conf.set("spark.default.parallelism", "4")
      val sc = new SparkContext(conf)
    

    val nameList = Array(Tuple2(1,"xuruyun"),Tuple2(2,"liangyongqi"),Tuple2(3,"wangfei"))
    val scoreList = Array(Tuple2(1,150),Tuple2(2,100),Tuple2(2,90))
    val name = sc.parallelize(nameList,3)
    val score = sc.parallelize(scoreList,2)
    
    val results = name.join(score)
    println("results.partitions.size:" + results.partitions.size)
    println("name.leftOuterJoin(score).partitions.size:" + name.leftOuterJoin(score).partitions.size)
    
    val temp = name.cogroup(score).collect()
    for(t <- temp){
      println(t)
    }
  }
}
