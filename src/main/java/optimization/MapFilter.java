package main.java.optimization;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class MapFilter {
	public static void main(String[] args) {
		/**
		 * SparkConf是设置spark运行时的环境变量，可以设置setMaster  setAppName，设置运行时所需要的资源情况
		 */
		SparkConf conf = new SparkConf().setAppName("JoinOperator")
				.setMaster("local");
		
		//SparkContext非常的重要，SparkContext是通往集群的唯一通道，在SparkContext初始化的时候会创建任务调度器
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<Integer, String>> nameList = Arrays.asList(
				new Tuple2<Integer,String>(1, "xuruyun"),
				new Tuple2<Integer,String>(2, "liangyongqi"),
				new Tuple2<Integer,String>(3, "wangfei"));
		
		JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(nameList);
		
		
		  JavaRDD<String> filter = nameRDD.map(new Function<Tuple2<Integer,String>, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<Integer, String> tuple) throws Exception {
				return tuple._2;
			}
		}).filter(new Function<String, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String str) throws Exception {
				return "xuruyun".equals(str)?true:false;
			}
		});
		
		List<String> collect = filter.collect();
		for (String string : collect) {
			System.out.println(string);
		}
		/**
		 * 
		 */
		while (true) {
		}
	}
}
