package main.scala.sql.loadsave

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

object SaveModeTest {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf()
        .setAppName("GenericLoadSave")
        .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
  
    //读取一个parquet文件
     /**
      * 将普通的文本格式转成parquet格式
      * 
      * val lineRDD = sc.textFile()
      * 将lineRDD变成DF
      * DF.save(parquet)
      */
     
    val usersDF = sqlContext.read.format("parquet").load("hdfs://hadoop1:9000/input/users.parquet")
    usersDF.save("hdfs://hadoop1:9000/output/users1.json", "json", SaveMode.Overwrite)
    
  }
}