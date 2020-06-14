package main.java.sql.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * @author Administrator
 *
 */
public class ParquetLoadData {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("ParquetLoadData")
				.setMaster("local");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		DataFrame usersDF = sqlContext.read().format("parquet").load("hdfs://hadoop1:9000/input/users.parquet");
//		DataFrame usersDF1 = sqlContext.read().parquet("hdfs://hadoop1:9000/input/users.parquet");
		 
		usersDF.registerTempTable("t");
		DataFrame resultDF = sqlContext.sql("SELECT * FROM t WHERE name = 'Alyssa'");
		resultDF.write().format("json").mode(SaveMode.Ignore).save("hdfs://hadoop1:9000/output/result.json");
		
		sc.stop();
	}
	
}
