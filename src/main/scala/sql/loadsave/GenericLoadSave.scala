package main.scala.sql.loadsave

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode


object GenericLoadSave {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setAppName("GenericLoadSave")
        .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
  
    //读取一个parquet文件
    val usersDF = sqlContext.read.format("parquet").load("hdfs://hadoop1:9000/input/users.parquet")

    usersDF.write.mode(SaveMode.Overwrite).format("json").save("hdfs://hadoop1:9000/output/namesAndFavColors_scala")  
    sc.stop()
  }
}