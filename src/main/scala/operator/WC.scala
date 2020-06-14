package main.scala.operator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WC {
  
  def main(args: Array[String]): Unit = {
    /**
     * SparkConf 是设置Spark运行时的环境变量，实际在这里面还可以设置Spark运行时所需要的资源情况。
     */
     val conf = new SparkConf()
       .setMaster("local")
       .setAppName("word哥")
     
       /**
        * 通过传入SparkConf创建一个SparkCotext，SparkContext是通往集群的唯一通道，为什么是通道？
        * 	1、发送task
        * 	2、接受计算结果
        * 	3、接受运行时的信息
        * 同时说明在SparkContext初始化的时候会创建任务调度器。
        */
      val sc = new SparkContext(conf)
     /**
      * 将文件内容加载到RDD中。
      */
     val linesRDD = sc.textFile("cs")
     
     val wordsRDD = linesRDD.flatMap { _.split(" ") }
     
     val pairRDD = wordsRDD.map { (_,1) }
     
     val resultRDD = pairRDD.reduceByKey(_+_)
     
     resultRDD.sortBy(_._2).foreach(println)
     
     /**
      * 到这一步，程序就执行了吗？
      */
    
  }
}