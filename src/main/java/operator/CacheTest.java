package main.java.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class CacheTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("WC")
				.setMaster("local");
			
		JavaSparkContext jsc = new JavaSparkContext(conf);
		jsc.setCheckpointDir("d://checkpoing/20170516");
		
		
		JavaRDD<String> lineRDD = jsc.textFile("cache");
		
		lineRDD.checkpoint();
		 
		lineRDD.count();
		
		jsc.stop();
			
	}
}
