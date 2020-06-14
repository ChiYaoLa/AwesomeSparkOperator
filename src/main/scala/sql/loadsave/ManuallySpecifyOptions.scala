package main.scala.sql.loadsave

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode

object ManuallySpecifyOptions {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setAppName("ManuallySpecifyOptions")  
        .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
  
    val peopleDF = sqlContext.read.format("json").load("hdfs://hadoop1:9000/input/people.json")
    peopleDF.select("name").write.format("parquet").mode(SaveMode.Append).save("hdfs://hadoop1:9000/output/peopleName_scala")   
    
  }
  
}