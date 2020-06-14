package main.scala.sql.createdf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DataTypes
import java.util.List
import org.apache.spark.sql.Column
import org.apache.spark.sql.Column
import org.apache.spark.sql.Column

object DataFrameOpsFromJsonRdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() //创建sparkConf对象
    conf.setAppName("My First Spark App") //设置应用程序的名称，在程序运行的监控页面可以看到名称
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    
    val sqlContext = new SQLContext(sc)
    
    val infos = Array("{'name':'zhangsan', 'age':55}","{'name':'lisi', 'age':30}","{'name':'wangwu', 'age':19}")
    val scores = Array("{'name':'zhangsan', 'score':155}","{'name':'lisi', 'score':130}")
    
    val infoRdd = sc.parallelize(infos)
    val scoreRdd = sc.parallelize(scores)
    
    val infoDF = sqlContext.read.json(infoRdd)
    val scoreDF = sqlContext.read.json(scoreRdd)
    
//    infoDF.registerTempTable("people")
    
 
    infoDF.join(scoreDF, infoDF("name").===(scoreDF("name"))).select(infoDF("name"),infoDF("age"),scoreDF("score")).show()
    
    
     val sql = "SELECT a.name,a.age,b.score FROM info a JOIN score b ON (a.name=b.name)"
    
    infoDF.registerTempTable("info")
    scoreDF.registerTempTable("score")
    
    sqlContext.sql(sql).show()
    
    
    
    
   /* df.show()

    df.printSchema()
    
    df.select("name").show()
    
    df.select(df("name"), df("age")+10).show()
    
    df.filter(df("age")>10).show()
    
    df.groupBy("age").count.show()*/
  }
}