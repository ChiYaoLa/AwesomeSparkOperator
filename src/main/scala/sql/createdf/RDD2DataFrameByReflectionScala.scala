package main.scala.sql.createdf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object RDD2DataFrameByReflectionScala {
  case class Person(name: String, age: Int)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() //创建sparkConf对象
    conf.setAppName("My First Spark App") //设置应用程序的名称，在程序运行的监控页面可以看到名称
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    
    val people = sc.textFile("Peoples.txt").map(_.split(",")).map(p => Person(p(1), p(2).trim.toInt)).toDF()
    
    
    people.registerTempTable("people")
    
    
    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 6 AND age <= 19")
    
    /**
     * 对dataFrame使用map算子后，返回类型是RDD<Row>
     */
    teenagers.map(t => "Name: " + t(0)).foreach(println)
    
    // or by field name:
    teenagers.map(t => "Name: " + t.getAs[String]("name")).foreach(println)
  }
}
