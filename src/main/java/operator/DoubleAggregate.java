package main.java.operator;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;


import scala.Tuple2;
import scala.collection.generic.BitOperations.Int;


public class DoubleAggregate {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JoinOperator")
				.setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Tuple2<String,Integer>> list = Arrays.asList(
				new Tuple2<String,Integer>("hello",1),
				new Tuple2<String,Integer>("hello",1),
				new Tuple2<String,Integer>("hello",1),
				new Tuple2<String,Integer>("hello",1)
				);
		
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
		
		JavaPairRDD<String, Integer> prefixRDD = rdd.mapToPair(new PairFunction<Tuple2<String,Integer>, String,Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
				 String word = tuple._1;
				 Integer count = tuple._2;
				 Random _random = new Random();
				 int prefix = _random.nextInt(2);
				 String key = prefix+"_"+word;
				return new Tuple2<String, Integer>(key, count);
			}
		});
		
		JavaPairRDD<String, Integer> _tmpRDD = prefixRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		},2);
		
		List<Tuple2<String, Integer>> collect = _tmpRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
				String _key = tuple._1;
				Integer count = tuple._2;
				String key = _key.split("_")[1];
				return new Tuple2<String, Integer>(key,count);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		}).collect();
		
		for (Tuple2<String, Integer> tuple2 : collect) {
			System.out.println(tuple2);
		}
	}
}
