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
public class GenericLoadSave {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf() 
				.setAppName("GenericLoadSave")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
	
		DataFrame usersDF = sqlContext.read().load(	"hdfs://hadoop1:9000/input/users.parquet");
		//没有指定format  就是写入到磁盘的数据格式     默认是parquet
		usersDF.select("name", "favorite_color").write().mode(SaveMode.Overwrite).save("hdfs://hadoop1:9000/output/namesAndFavColors_scala");   
		
		DataFrame pDF = sqlContext.read().parquet("hdfs://hadoop1:9000/output/namesAndFavColors_scala");
		pDF.show();
	}
	
}
