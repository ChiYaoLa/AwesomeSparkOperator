package main.java.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class FirstApp {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("FirstApp");
		sparkConf.setMaster("local");
		
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> rdd = sc.textFile("cs");
		
		 JavaRDD<String> flatMapRDD = rdd.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String line) throws Exception {
				List<String> list = new ArrayList<>();
				list = Arrays.asList(line.split(" "));
				return list;
			}
		});
		
		 List<String> collect = flatMapRDD.collect();
		 for (String string : collect) {
			System.out.println(string);
		}
		 
		 
		
		sc.stop();
	}
}
