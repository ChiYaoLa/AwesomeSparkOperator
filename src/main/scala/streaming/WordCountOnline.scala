package main.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Durations
import org.apache.spark.storage.StorageLevel

object WordCountOnline {
  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]")
    sparkConf.setAppName("WordCountOnline")
    
    
    val ssc = new StreamingContext(sparkConf,Durations.seconds(5))
    
    val linesDStream = ssc.socketTextStream("hadoop1", 9999, StorageLevel.MEMORY_AND_DISK)
    
    val wordsDStream = linesDStream.flatMap { _.split(" ") }
    
    val pairDStream = wordsDStream.map { (_,1) }
    
    val resultDStream = pairDStream.reduceByKey(_+_)
    
    resultDStream.print()
    
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
    
    
  }
}