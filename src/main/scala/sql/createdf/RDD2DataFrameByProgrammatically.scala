package main.scala.sql.createdf

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import java.util.Arrays.ArrayList
import java.util.ArrayList

object RDD2DataFrameByProgrammatically {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() //创建sparkConf对象
    conf.setAppName("My First Spark App") //设置应用程序的名称，在程序运行的监控页面可以看到名称
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val people = sc.textFile("scores.txt")
    
    val schemaString = "clazz:String score:Integer"
    //如果schema中制定了除String以外别的类型   在构建rowRDD的时候要注意指定类型     例如： p(2).toInt 
    val rowRDD = people.map(_.split("\t")).map(p => Row(p(0), p(1).toInt))
    
    val schema =
      StructType(schemaString.split(" ").map(fieldName => StructField(fieldName.split(":")(0), if (fieldName.split(":")(1).equals("String")) StringType else IntegerType, true)))
//    val structFields = Array(StructField("clazz",StringType,true),StructField("score",IntegerType))
//    val schema = StructType(structFields)
    
    
    
    //  val arr = Array(StructField("name",StringType,true),StructField("age",IntegerType,true))
    //  val schema = StructType.apply(arr)
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    peopleDataFrame.printSchema()
    peopleDataFrame.show()
   /* peopleDataFrame.registerTempTable("clazzScore")
    val results = sqlContext.sql("SELECT score,clazz FROM clazzScore")
    //  results.map(t => "age: " + t(0)).collect().foreach(println)
    results.map(t => "clazz: " + t.getAs[String]("clazz")+"\tscore:"+t.getAs[Integer]("score")).foreach(println)*/
    
  }
}












