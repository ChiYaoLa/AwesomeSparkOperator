package main.java.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class JavaOperator {
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("JavaOperator")
				//设置运行模式
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		List<String> list = Arrays.asList("hello bjsxt","hello shsxt");
		JavaRDD<String> rdd = sc.parallelize(list);
		
		List<Tuple2<Integer, String>> list1 = Arrays.asList(
				new Tuple2<Integer,String>(1, "bjsxt"),
				new Tuple2<Integer,String>(2, "bjsxt"),
				new Tuple2<Integer,String>(3, "bjsxt")
				);
		
		/**
		 * 在java中通过一个本地集合创建一个RDD的时候需要注意：
		 * 	1、如果创建KV格式的RDD parallelizePairs
		 * 	2、如果创建非KV格式的RDD parallelize
		 * scala:
		 * 		无论创建是否是KV格式的RDD，都是用parallelize
		 */
		JavaPairRDD<Integer, String> pairRDD = sc.parallelizePairs(list1);
		
		
		JavaRDD<Integer> mapRDD = rdd.map(new Function<String, Integer>() {

			@Override
			public Integer call(String v1) throws Exception {
				//v1代表的是rdd中每一条记录
				return 1;
			}
		});
		
		JavaRDD<String> flatMap = rdd.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterable<String> call(String t) throws Exception {
				return Arrays.asList();
			}
		});
		
		long count = flatMap.count();
		System.out.println("count:" + count);
		
		
		
		mapRDD.foreach(new VoidFunction<Integer>() {
			
			@Override
			public void call(Integer t) throws Exception {
				 System.out.println(t);
			}
		});
		
		
		pairRDD.filter(new Function<Tuple2<Integer,String>, Boolean>() {

			@Override
			public Boolean call(Tuple2<Integer, String> v1) throws Exception {
				return  v1._1 != 1;
			}
		}).foreach(new VoidFunction<Tuple2<Integer,String>>() {

			@Override
			public void call(Tuple2<Integer, String> t) throws Exception {
				System.out.println(t);
			}
		});
		
		
		
	}
}
