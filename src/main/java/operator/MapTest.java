package main.java.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

public class MapTest {
	public static void main(String[] args) {
		/**
		 * SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置运行模式  Local Standlone、yarn模式哦
		 	AppName（可以在WEB UI中看到）   
		 	还可以设置Spark运行时的资源要求
		 */
		SparkConf conf = new SparkConf().setAppName("JoinOperator")
				.setMaster("local");
		
		/**
		 * 基于SparkConf的对象可以创建出来一个SparkContext Spark上下问
		 * SparkContetx是通往集群的 唯一通道。  SparkContext在创建的时候他还会创建任务调器
		 */
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<String> list = Arrays.asList("hello,bjsxt","hello,xuruyun");
		
		JavaRDD<String> linesRDD = sc.parallelize(list);
		
		JavaRDD<Object> mapRDD = linesRDD.map(new Function<String, Object>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Object call(String v1) throws Exception {
				// TODO Auto-generated method stub
				return v1.split(",");
			}
		});
		
		JavaRDD<String> flatMapRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterable<String> call(String t) throws Exception {
				 
				return Arrays.asList(t.split(","));
			}
		});
		
		
		List<Object> collect = mapRDD.collect();
		
		for(Object obj : collect)
			System.out.println(obj);
		
		
		List<String> collect2 = flatMapRDD.collect();
		for(String str : collect2)
			System.out.println(str);
	}
}
