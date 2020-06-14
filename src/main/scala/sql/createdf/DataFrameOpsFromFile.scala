package main.scala.sql.createdf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object DataFrameOpsFromFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() //创建sparkConf对象
    conf.setAppName("My First Spark App") //设置应用程序的名称，在程序运行的监控页面可以看到名称
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    
    val sqlContext = new SQLContext(sc)
    
//    val df = sqlContext.read.json("hdfs://hadoop1:9000/input/people.json")
     val df = sqlContext.read.json("people.json")
     
     //将DF注册成一张临时表，这张表是逻辑上的  并不会数据落地
    //people这是临时表的表明，    后面的SQL直接FROM 这个表名
    df.registerTempTable("people")
    sqlContext.sql("select * from people where age > 20").show()
    df.show()
//    df.show()
//
//    df.printSchema()
//    
//     df.select("name").show()
//     
//     //SELECT name ,age+10 from table
//     df.select(df("name"), df("age").plus(10)).show()
//     
//     //SELECT * FROM table WHERE age > 10
//     df.filter(df("age")>10).show()
//     
//     //SELECT count（*） FROM table GROUP BY age
//     df.groupBy("age").count.show() 
  }
}