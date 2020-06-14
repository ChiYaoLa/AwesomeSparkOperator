package main.scala.sql.parquet

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

/**
 * @author Administrator
 */
object ParquetLoadData {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setAppName("ParquetLoadData")  
        .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    val usersDF = sqlContext.read.parquet("hdfs://hadoop1:9000/input/users.parquet")  
    
    usersDF.show()
    
    usersDF.registerTempTable("users")
    val userNamesDF = sqlContext.sql("select name from users")  
    userNamesDF.rdd.map { row => "Name: " + row(0) }.collect()
        .foreach { userName => println(userName) }   
  }
}