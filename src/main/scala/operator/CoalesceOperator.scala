package main.scala.operator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer

object CoalesceOperator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CoalesceOperator").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val dataArr = Array("xuruyun1","xuruyun2","xuruyun3"
				,"xuruyun4","xuruyun5","xuruyun6"
				,"xuruyun7","xuruyun8","xuruyun9"
				,"xuruyun10","xuruyun11","xuruyun12")
		val dataRdd = sc.parallelize(dataArr, 3);
    
    /**
     * PartitionId0	Content:xuruyun1
     * PartitionId0	Content:xuruyun1
     * PartitionId0	Content:xuruyun1
     * PartitionId0	Content:xuruyun1
     * PartitionId0	Content:xuruyun1
     * PartitionId0	Content:xuruyun1
     */
    val result = dataRdd.mapPartitionsWithIndex((index,x) => {
        val list = ListBuffer[String]()
        println("partitionId:"+index)
        while (x.hasNext) {
          val v = x.next
          println(v)
          list+=("PartitionId"+index+"\tContent:"+v)
        }
        list.iterator
      }) 
      
      val coalesceRdd = result.coalesce(2,false)
    
      val results = coalesceRdd.mapPartitionsWithIndex((index,x) => {
        val list = ListBuffer[String]()
        while (x.hasNext) {
          list+=("PartitionId"+index+"\tContent:"+x.next)
        }
        list.iterator
      })
      
     println(results.partitions.size)
     val resultArr = results.collect()
      for(a<-resultArr){
        println(a)
      }
  }
}