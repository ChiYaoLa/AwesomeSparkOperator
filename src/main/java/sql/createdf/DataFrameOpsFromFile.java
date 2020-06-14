package main.java.sql.createdf;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 使用Java的方式开发实战对DataFrame的操作
 * 
 * @author zfg
 *读取json格式的文件，  文件内容不能是嵌套的
 */
public class DataFrameOpsFromFile {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("DataFrameOps").setMaster("local");
		/**
		 * 通往集群的唯一通道
		 */
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/**
		 * 
		 */
		SQLContext sqlContext = new SQLContext(sc);
		
// 		DataFrame df = sqlContext.read().format("json").load("people.json");
		/**
		 * DataFrame的底层是一个个的RDD
		 * 只不过DataFrame底层RDD的泛型是Row
		 * DataFrame = RDD<Row>
		 */
		
  		DataFrame df = sqlContext.read().json("people.json");
  		/**
  		 * 默认只是显示这个DF里面的前十行数据   
  		 * 类似Spark core里面的action类算子
  		 */
  		 df.show();
  		
		/* df.printSchema();
		 
		 df.select("age").show();
		 //SELECT name,age+10 as plusAge FROM table;
		  df.select(df.col("name"),df.col("age").plus(10).as("plusAge")).show();
		  //SELECT * FROM p WHERE age > 10
		   df.filter(df.col("age").gt(10)).show();
		   //SELECT COUNT(*) FROM p GROUP BY age 
//		 df.groupBy(df.col("age")).count().show(); 
		 
		 
		 *//**
		  * 将DataFrame注册成一张临时表
		  * p这个表，会物化到磁盘吗？ 这个表只是逻辑上的。
		  *//*
		 df.registerTempTable("p");
		 sqlContext.sql("SELECT * FROM p WHERE age > 10").show();  */
		 
		 
		 
 		/*//select age from table;
  		 df.select("age").show();
		//类似 SQL的select from table;
		
		//desc table
		
		//select name from table;
 		  df.select("name").show();
//		
//		//select name,age+10 from table;
 	 	df.select(df.col("name"),df.col("age").plus(10)).show();
		
	 	//select * from table where age > 20
 		 df.filter(df.col("age").gt(20)).show();
 		
//		//select count(1) from table group by age;
 		df.groupBy(df.col("age")).count().show(); */    
	}
}
