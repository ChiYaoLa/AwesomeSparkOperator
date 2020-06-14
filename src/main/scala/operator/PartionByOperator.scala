package main.scala.operator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.Partitioner

 
object PartionByOperator {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JoinOperator")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val nameList = Array(Tuple2(1,"xuruyun"),Tuple2(2,"liangyongqi"),Tuple2(3,"wangfei"))
    val name = sc.parallelize(nameList,1)
   
    val result = name.partitionBy(new UDPartitioner(2))
    
    result.mapPartitionsWithIndex((index,iterator) => {
       println("partitionId:"+index)
       while(iterator.hasNext){
         println(iterator.next())
       }
      iterator
    }, false).count
    
  }
  
}

class UDPartitioner(numParts: Int) extends Partitioner {
	   override def numPartitions = numParts
	   override def getPartition(key: Any): Int = {
	     key.toString().toInt%numPartitions
}
}


