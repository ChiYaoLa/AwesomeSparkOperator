package main.scala.sql.parquet

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ParquetDataSourcePartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("GenericLoadSave")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val usersDF = sqlContext.read.format("parquet").load("hdfs://hadoop1:9000/users/gender=male")

    //     val usersDF = sqlContext.read.parquet("users/gender=male/country=US/users.parquet")
    usersDF.printSchema()
    usersDF.show()
  }
}