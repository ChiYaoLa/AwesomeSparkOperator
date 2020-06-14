package main.java.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

// 排序

public class SortByKeyOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("SortByKeyOperator")
				.set("spark.shuffle.manager", "hash")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 模拟集合
		List<Tuple2<Integer, String>> scoreList = Arrays.asList(
				new Tuple2<Integer,String>(100, "liangyongqi"),
				new Tuple2<Integer,String>(150, "xuruyun"),
				new Tuple2<Integer,String>(90, "wangfei"));
		
		// 并行化集合
		JavaPairRDD<Integer, String> scores = sc.parallelizePairs(scoreList);
		
		JavaPairRDD<Integer, String> results = scores.sortByKey(false);
		
		results.foreach(new VoidFunction<Tuple2<Integer,String>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, String> tuple) throws Exception {
				System.out.println(tuple._2);
			}
		});
		
		sc.close();
	}
}
