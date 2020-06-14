package main.scala.operator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner

object MapValuesOperator {
  def main(args: Array[String]): Unit = {
     val sparkConf = new SparkConf().setAppName("MapValuesOperator")
          .setMaster("local")
      val sc = new SparkContext(sparkConf)
     val links = sc.parallelize(List(("A","Q"),("B","w"),("C","r"),("D","T"))).partitionBy(new HashPartitioner(100)).persist()  
       var ranks=links.mapValues(v=>1.0)  
       val results = ranks.collect()
       for(result<- results){
         println(result)
       }
     while(true){}
      sc.stop()
  }
}