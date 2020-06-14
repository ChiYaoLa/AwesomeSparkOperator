package main.scala.operator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

/**
 * 测试RDD缓存
 */
object CachedTest {
  def main(args: Array[String]): Unit = {
      //创建Spark运行时的配置对象，在配置对象里面可以设置APP name，集群URL以及运行时各种资源需求
      val sparkConf = new SparkConf().setAppName("MapOperator")
      .setMaster("local")

      //创建SparkContext上下文环境，通过传入配置对象实例化一个SparkContext
      val sc = new SparkContext(sparkConf)  
      
      var linesRdd = sc.textFile("hs_err_pid5848.log")
      
      linesRdd = linesRdd.persist(StorageLevel.MEMORY_ONLY)
      
      val startTime = System.currentTimeMillis()
      val lineCount = linesRdd.count()
      val endTime = System.currentTimeMillis()
      println("总共有"+lineCount+"条记录, 计算耗时:"+( endTime-startTime))
      
      
      val startCachedTime = System.currentTimeMillis()
      val linesCountCached = linesRdd.count()
      val endCachedTime = System.currentTimeMillis()
      println("总共有"+lineCount+"条记录, 计算耗时:"+(endCachedTime-startCachedTime))
      
      
      
    
  }
}