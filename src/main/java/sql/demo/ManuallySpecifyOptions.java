package main.java.sql.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
/**
 * 手动指定数据源类型
 * @author Administrator
 *
 */
public class ManuallySpecifyOptions {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()   
				.setAppName("ManuallySpecifyOptions")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//通过传入的SparkContext创建一个SQLContext
		SQLContext sqlContext = new SQLContext(sc);
		
		//通过读取HDFS上面的json文件生成一个DataFrame
		DataFrame peopleDF = sqlContext.read().json("hdfs://hadoop1:9000/input/people.json");
		
		peopleDF.select("name").write().mode(SaveMode.Overwrite).save("hdfs://hadoop1:9000/output/peopleName_java");  
		DataFrame pDF = sqlContext.read().parquet("hdfs://hadoop1:9000/output/peopleName_java");
		pDF.show();
	}
	
}
