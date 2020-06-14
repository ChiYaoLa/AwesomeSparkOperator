package main.scala.sql.demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._


object DailyUV {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setMaster("local")  
        .setAppName("DailyUV")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._
    
    val userAccessLog = Array(
        "2016-9-01,1122",
        "2016-9-01,1122",
        "2016-9-01,1123",
        "2016-9-01,1124",
        "2016-9-01,1124",
        "2016-9-02,1122",
        "2016-9-02,1121",
        "2016-9-02,1123",
        "2016-9-02,1123");
    val userAccessLogRDD = sc.parallelize(userAccessLog, 5)
    
    val userAccessLogRowRDD = userAccessLogRDD
        .map { log => Row(log.split(",")(0), log.split(",")(1).toInt) }

    val structType = StructType(Array(
        StructField("date", StringType, true),
        StructField("userid", IntegerType, true)))  

    val userAccessLogRowDF = sqlContext.createDataFrame(userAccessLogRowRDD, structType)  
    
   
    userAccessLogRowDF.groupBy("date")
        .agg('date, countDistinct('userid))  
        .map { row => Row(row(1), row(2)) }   
        .collect()
        .foreach(println)  
  }
}