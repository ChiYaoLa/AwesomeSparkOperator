package main.java.operator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import groovy.lang.Tuple;
import scala.Tuple2;

public class BroadCastJoin {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JoinOperator")
				.setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 模拟集合
		List<Tuple2<Integer, String>> nameList = Arrays.asList(
				new Tuple2<Integer,String>(1, "xuruyun"),
				new Tuple2<Integer,String>(2, "liangyongqi"),
				new Tuple2<Integer,String>(3, "wangfei"));
		
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer,Integer>(1, 150),
				new Tuple2<Integer,Integer>(2, 100),
				new Tuple2<Integer,Integer>(1, 150),
				new Tuple2<Integer,Integer>(2, 100)
				 );
		
		 JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(nameList);
		 JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scoreList);
		 
		 JavaPairRDD<Integer, Tuple2<String, Integer>> join = nameRDD.join(scoreRDD);
		List<Tuple2<Integer, Tuple2<String, Integer>>> collect = join.collect();
		for (Tuple2<Integer, Tuple2<String, Integer>> tuple2 : collect) {
			System.out.println(tuple2);
		}
		
		Map<Integer, String> nameMap = new HashMap<>();
		for (Tuple2<Integer, String> tuple2 : nameRDD.collect()) {
			nameMap.put(tuple2._1, tuple2._2);
		}
		
		/**
		 * 使用广播变量来实现join的功能。
		 */
		final Broadcast<Map<Integer, String>> nameBroadCast = sc.broadcast(nameMap);
		
		JavaPairRDD<Integer, Tuple2<String, Integer>> mapToPair = scoreRDD.mapToPair(new PairFunction<Tuple2<Integer,Integer>, Integer, Tuple2<String,Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Tuple2<String, Integer>> call(Tuple2<Integer, Integer> tuple) throws Exception {
				Map<Integer, String> nameMap = nameBroadCast.value();
				Integer id = tuple._1;
				Integer score = tuple._2;
				String name = nameMap.get(id);
				
				return new Tuple2<Integer, Tuple2<String,Integer>>(id, new Tuple2<String,Integer>(name,score));
			}
		});
		List<Tuple2<Integer, Tuple2<String, Integer>>> collect2 = mapToPair.collect();
		for (Tuple2<Integer, Tuple2<String, Integer>> tuple2 : collect2) {
			System.out.println(tuple2);
		}
		
	}
}
