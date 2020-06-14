package main.java.sql.createdf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.DU;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.GroupedData;
import org.apache.spark.sql.SQLContext;

import groovy.sql.Sql;
import scala.Tuple2;

/**
 * 使用Java的方式开发实战对DataFrame的操作
 * 
 * @author zfg
 *
 *
 */
public class DataFrameOpsFromJsonRdd {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("DataFrameOps").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		/**
		 * 若想使用SparkSQL必须创建SQLContext    必须是传入SparkContext    不能是SparkConf
		 */
		SQLContext sqlContext = new SQLContext(sc);
		
		/**
		 * 创建一个本地的集合  集合中元素的格式   json        类型String
		 */
		List<String> nameList = Arrays.asList(
						"{'name':'zhangsan', 'age':55}",
						"{'name':'lisi', 'age':30}",
						"{'name':'lisisi', 'age':30}",
						"{'name':'wangwu', 'age':19}");
		
		List<String> scoreList = Arrays.asList(
				"{'name':'zhangsan','score':100}",
				"{'name':'lisi','score':99}" );
		
		/**
		 * 并行化成一个rdd  现在rdd中元素格式    json格式
		 */
		JavaRDD<String> nameRDD = sc.parallelize(nameList);
		JavaRDD<String> scoreRDD = sc.parallelize(scoreList);
		
		DataFrame nameDF = sqlContext.read().json(nameRDD);
		DataFrame scoreDF = sqlContext.read().json(scoreRDD);
		
		/**
		 * SELECT nameTable.name,nameTable.age,scoreTable.score
		 * 		FROM nameTable JOIN nameTable ON (nameTable.name == scoreTable.name)
		 */
		nameDF.join(scoreDF, nameDF.col("name").$eq$eq$eq(scoreDF.col("name")))
			.select(nameDF.col("name"),nameDF.col("age"),scoreDF.col("score")).show();		
		
		
		
		
	 	nameDF.registerTempTable("name");
		scoreDF.registerTempTable("score");
		String sql = "SELECT name.name,name.age,score.score "
				+ "FROM name join score ON (name.name = score.name)";
		
		sqlContext.sql(sql).show(); 
		
		
//		scoreDF.show();
		
		/*//类似 SQL的select * from table;
		df.show();
		
	  	//desc table
		df.printSchema();
		
		//select name from table;
		df.select("name").show();
		
		//select name,age+10 from table;
		df.select(df.col("name"),df.col("age").plus(10)).show();
		
		//select * from table where age > 20
		df.filter(df.col("age").gt(20)).show();
		
		//select count(1) from table group by age;
		 df.groupBy(df.col("age")).count().show();  */
	}
}
