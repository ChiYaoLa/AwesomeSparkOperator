package main.scala.operator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer

object RepartitionOperator {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RepartitionOperator")
      .setMaster("local[1]")
    val sc = new SparkContext(conf)
    
    
    val rdd = sc.textFile("hs_err_pid5848.log", 3)
    println("rdd.partitions.size:" + rdd.partitions.size)
    
    
    rdd.mapPartitionsWithIndex((index,itertor)=>{
      println("partitionId:"+index)
      while(itertor.hasNext){
        val v = itertor.next
        println(v)
      }
      itertor 
    }, false).count()
    
    
    val repartitionRDD = rdd.repartition(5)
    
    //repartition(numpartitions) = coalesce(numpartitions,true)
    val coaesceRDD = rdd.coalesce(2, false)
    
    println("coaesceRDD.partitions.size:" + coaesceRDD.partitions.size)
    
    println("repartitionRDD.partitions.size:"+repartitionRDD.partitions.size)
    
    sc.stop()
    
    

  }
}