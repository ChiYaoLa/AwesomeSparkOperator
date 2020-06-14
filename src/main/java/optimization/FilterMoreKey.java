package main.java.optimization;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class FilterMoreKey {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JoinOperator")
				.setMaster("local");
		
		//SparkContext非常的重要，SparkContext是通往集群的唯一通道，在SparkContext初始化的时候会创建任务调度器
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<Integer, String>> nameList = Arrays.asList(
				new Tuple2<Integer,String>(1, "xuruyun"),
				new Tuple2<Integer,String>(2, "liangyongqi"),
				new Tuple2<Integer,String>(3, "wangfei1"),
				new Tuple2<Integer,String>(3, "wangfei2"),
				new Tuple2<Integer,String>(3, "wangfei3"),
				new Tuple2<Integer,String>(3, "wangfei4"),
				new Tuple2<Integer,String>(3, "wangfei5"),
				new Tuple2<Integer,String>(3, "wangfei6"),
				new Tuple2<Integer,String>(3, "wangfei7"),
				new Tuple2<Integer,String>(3, "wangfei9"));
		
		JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(nameList);
		
		JavaPairRDD<Integer, String> sample = nameRDD.sample(false, 0.5);
		 
		final Integer skewedKey = sample.mapToPair(new PairFunction<Tuple2<Integer,String>, Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
				 
				return new Tuple2<Integer, Integer>(tuple._1, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = -8834240548600166822L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 +v2 ;
			}
		}).mapToPair(new PairFunction<Tuple2<Integer,Integer>, Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> tuple) throws Exception {
				return new Tuple2<Integer, Integer>(tuple._2,tuple._1);
			}
		}).sortByKey().take(1).get(0)._2;
		
		JavaPairRDD<Integer, String> filtered = nameRDD.filter(new Function<Tuple2<Integer,String>, Boolean>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<Integer, String> tuple) throws Exception {
				Integer key = tuple._1;
				if(skewedKey.intValue() == key.intValue()){
					return false;
				}
				return true;
			}
		});
		
		List<Tuple2<Integer, String>> collect = filtered.collect();
		for (Tuple2<Integer, String> tuple2 : collect) {
			System.out.println(tuple2);
		}
		
	}
}	
