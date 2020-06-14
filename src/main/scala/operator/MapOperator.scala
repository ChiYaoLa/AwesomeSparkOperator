package main.scala.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2016/6/7.
  */
object MapOperator {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MapOperator").setMaster("local")
    val sc = new SparkContext(conf)
   

    val array = Array("hello,xuruyun","hello,liangyongqi")
    
    val nameRdd = sc.parallelize(array)
    
    val mapRDD = nameRdd.map { _.split(",") }
    val mapResults = mapRDD.collect()
    for( result <- mapResults)
      println(result)
      
     val flatMapRDD = nameRdd.flatMap{ _.split(",") }  
     val flatMapResults = flatMapRDD.collect()
     for( result <- flatMapResults)
      println(result)
//    val results = nameRdd.map( x => {
//        println("map:"+x)
//         "Hello\t"+x
//    }).filter { x => println("filter:" +x );  true}.count
//    
    
  }
}
