package main.scala.operator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * cogroup算子演示
 */
object CogroupOperator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CogroupOperator")
    .setMaster("local")
    val sc = new SparkContext(sparkConf)
    
    val nameList = List((1,"wangbaoqiang"),(2,"marong"),(3,"songzhe"))
    val nameRdd = sc.parallelize(nameList)
    
    val scoreList = List((1,100),(2,59),(3,-1))
    val scoreRdd = sc.parallelize(scoreList)
    
    val gradeList = List((1,"三年级"),(2,"幼儿园"),(3,"五年级"))
    val gradeRdd = sc.parallelize(gradeList)
    
    val resultRdd = nameRdd.cogroup(scoreRdd,gradeRdd)
    
    val results = resultRdd.collect()
    for(result <- results){
      println(result)
      val rs  = result._2._3.seq
      for(r <- rs){
        println(r)
      }
    }
    
    sc.stop()
  }
}