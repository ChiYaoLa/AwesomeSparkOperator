package main.java.sql.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Parquet数据源之自动推断分区
 * @author Administrator
 * 类似hive里面的分区表的概念
 */
public class ParquetPartitionDiscovery {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("ParquetPartitionDiscovery")
				.setMaster("local");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		DataFrame usersDF = sqlContext.read().parquet(
				"hdfs://hadoop1:9000/users");
		usersDF.printSchema();
		usersDF.show();
		usersDF.registerTempTable("table1");
		sqlContext.sql("SELECT count(0) FROM table1 WHERE country = 'china'").show();
		sc.stop();
		
	}
}
