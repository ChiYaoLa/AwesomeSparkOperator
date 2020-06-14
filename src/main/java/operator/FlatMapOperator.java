package main.java.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

public class FlatMapOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster(
				"local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<String> lineList = Arrays.asList("hello xuruyun" , "hello xuruyun", "hello wangfei");
		JavaRDD<String> lines = sc.parallelize(lineList);
		
		// flatMap = flat + map
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
		});
		
		words.foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String result) throws Exception {
				System.out.println(result);
			}
		});
		
		sc.close();
	}
}
