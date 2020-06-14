package main.java.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class FilterOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster(
				"local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

		JavaRDD<Integer> results = numberRDD.filter(new Function<Integer, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Integer number) throws Exception {
						System.out.println("filter result:" + number);
						return true;
					}
				}).map(new Function<Integer, Integer>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1) throws Exception {
						 System.out.println("map result:" + v1);
						return v1;
					}
				});
		
		results.foreach(new VoidFunction<Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer result) throws Exception {
//				System.out.println(result);
			}
		});

		while (true) {
			
		}
//		sc.close();
	}
}
