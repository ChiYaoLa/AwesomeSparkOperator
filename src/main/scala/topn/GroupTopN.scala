package main.scala.topn

import org.apache.spark.{SparkContext, SparkConf}





object GroupTopN {
   def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GroupTopN").setMaster("local")
    val sc = new SparkContext(conf)
    
    val lines=sc.textFile("E:\\内部培训\\14 Spark\\SparkScalaOperator\\scores.txt")
    
    val lineList=lines.map(x=>(x.split("\t")(0),x.split("\t")(1).toInt)).groupByKey()
    
    val topList=lineList.map(x=>{
      x._2.toList.sortBy { x => -x }.take(3)
    })
    topList.foreach { println }
   }
}