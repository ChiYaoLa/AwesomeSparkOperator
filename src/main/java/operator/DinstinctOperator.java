package main.java.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class DinstinctOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SampleOperator").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<String,Integer>> names = Arrays.asList(new Tuple2<String,Integer>("xuruyun",1), new Tuple2<String,Integer>("liangyongqi",1), new Tuple2<String,Integer>("wangfei",1), new Tuple2<String,Integer>("xuruyun",1));
		JavaRDD<Tuple2<String, Integer>> parallelize = sc.parallelize(names);
		
		JavaRDD<Tuple2<String, Integer>> distinct = parallelize.distinct();
		List<Tuple2<String, Integer>> collect = distinct.collect();
		for (Tuple2<String, Integer> tuple2 : collect) {
			System.out.println(tuple2);
		}
		
		JavaPairRDD<String, Integer> parallelizePairs = sc.parallelizePairs(names);
		JavaPairRDD<String, Integer> distinct2 = parallelizePairs.distinct();
		List<Tuple2<String, Integer>> collect2 = distinct.collect();
		for (Tuple2<String, Integer> tuple2 : collect2) {
			System.out.println(tuple2);			
		}
		
		/*JavaPairRDD<String, Integer> mapToPair = nameRDD.mapToPair(new PairFunction<String, String,Integer>() {

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String str) throws Exception {
				
				return new Tuple2<String, Integer>(str, 1);
			}
		});
		
		JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1 + v2;
			}
		});
		
		JavaRDD<String> map = reduceByKey.map(new Function<Tuple2<String,Integer>, String>() {

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, Integer> tuple) throws Exception {
				// TODO Auto-generated method stub
				return tuple._1;
			}
		});
		
		map.foreach(new VoidFunction<String>() {

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String str) throws Exception {
				 
				System.out.println(str);
			}
		});*/
	}
}
