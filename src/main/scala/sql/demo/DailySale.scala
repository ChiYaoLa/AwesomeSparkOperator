package main.scala.sql.demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
 * 日期，销售额
 */
 
object DailySale {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setMaster("local")
        .setAppName("DailySale")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
  
    import sqlContext.implicits._
    
    val userSaleLog = Array(
        "2016-9-01,55,1122",
        "2016-9-01,23,1133",
        "2016-9-01,15,",
        "2016-9-02,56,1144",
        "2016-9-02,78,1155",
        "2016-9-03,113,1123")
    val userSaleLogRDD = sc.parallelize(userSaleLog, 5)
    
    val filteredUserSaleLogRDD = userSaleLogRDD
        .filter { log => if (log.split(",").length == 3) true else false }
      
    val userSaleLogRowRDD = filteredUserSaleLogRDD
        .map { log => Row(log.split(",")(0), log.split(",")(1).toInt) }
    
    val structType = StructType(Array(
        StructField("date", StringType, true),
        StructField("sale_amount", IntegerType, true)))
    
    val userSaleLogDF = sqlContext.createDataFrame(userSaleLogRowRDD, structType)  
    
    userSaleLogDF.groupBy("date")
        .agg('date, sum('sale_amount))
        .map { row => Row(row(1), row(2)) }
        .collect()
        .foreach(println)  
  }
  
}