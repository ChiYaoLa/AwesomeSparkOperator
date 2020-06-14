package main.scala.sql.parquet

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode

object ParquetDataSourceSchemaMerge {
  
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetDataSourceSchemaMerge").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._
    
    // 创建基本信息,写入一个parquet文件中
    val studentsWithNameAge = Array(("john",18),("april",19),("annie",17))
    val studentsWithNameAgeDF = sc.parallelize(studentsWithNameAge).toDF("name","age")
    studentsWithNameAgeDF.save("hdfs://hadoop1:9000/output/students", "parquet", SaveMode.Append)
    
    // 创建基本信息,写入一个parquet文件中
    val studentsWithNameScore = Array(("john",88),("wangfei",99),("liangyongqi",77))
    val studentsWithNameScoreDF = sc.parallelize(studentsWithNameScore).toDF("name","score")
    studentsWithNameScoreDF.save("hdfs://hadoop1:9000/output/students", "parquet", SaveMode.Append)
    
   val rDF = studentsWithNameScoreDF.join(studentsWithNameAgeDF,studentsWithNameScoreDF("name")===studentsWithNameAgeDF("name"))
//   rDF.show
   
 val joinRdd = studentsWithNameAgeDF.map (row => (row(0),row(1))).join(studentsWithNameScoreDF.map { row => (row(0),row(1))})
  for(a <- joinRdd.collect()){
    println(a)
  } 
   
    
//    val rdd = studentsWithNameAgeDF.rdd.map { x => (x.get(0),x.get(1))}
//      .join(studentsWithNameScoreDF.rdd.map { x => (x.get(0),x.get(1))})
//     for(a<-rdd.collect){
//       println(a)
//     }
//     println(rdd.count)
    
    
    
    val students = sqlContext.read.option("mergeSchema", "true").parquet("hdfs://hadoop1:9000/output/students")
    students.printSchema()
    students.show()
  }
}