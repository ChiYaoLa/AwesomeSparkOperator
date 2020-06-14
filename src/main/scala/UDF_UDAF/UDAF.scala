package main.scala.UDF_UDAF

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

/**
 * @author Administrator
 */
object UDAF {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setMaster("local") 
        .setAppName("UDAF")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val names = Array("yarn", "Marry", "Jack", "Tom", "Tom", "Tom")  
    
    val namesRDD = sc.parallelize(names, 5) 
    
    val namesRowRDD = namesRDD.map { name => Row(name) }
    
    val structType = StructType(Array(StructField("name", StringType, true)))  
    val namesDF = sqlContext.createDataFrame(namesRowRDD, structType) 
    
    namesDF.registerTempTable("names")  

    sqlContext.udf.register("strCount", new StringCount) 
    
    // 使用自定义函数
    sqlContext.sql("select name,strCount(name) from names group by name")  
        .collect()
        .foreach(println)  
  }
}