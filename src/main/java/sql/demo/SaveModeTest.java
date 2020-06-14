package main.java.sql.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * 读取json或者parquet文件创建一个DataFrame
 * DataFrame存储到某一个路径下，默认存储格式是parquet
 * SaveMode.Overwrite：重写
 * Append:
 * @author root
 * 
 */

public class SaveModeTest {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()   
				.setAppName("SaveModeTest")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
	
		DataFrame peopleDF = sqlContext.read().json("hdfs://hadoop1:9000/input/people.json"); 
		
 		peopleDF.write().format("parquet").mode(SaveMode.Overwrite).save("hdfs://hadoop1:9000/output/0306parquet"); 
		
//		DataFrame df = sqlContext.read().parquet("hdfs://hadoop1:9000/output/0306parquet");
//		df.show();
	}
}





