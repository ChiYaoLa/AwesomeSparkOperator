package main.java.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class SampleOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SampleOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<String> names = Arrays
				.asList(
						"bjsxt",
						"bjsxt",
						"bjsxt",
						"bjsxt",
						"bjsxt",
						"bjsxt",
						"bjsxt",
						"bjsxt",
						"bjsxt",
						"bjsxt",
						"bjsxt",
						"bjsxt",
						"bjsxt",
						"bjsxt",
						"bjsxt",
						"shsxt",
						"gzsxt"
						);
		JavaRDD<String> nameRDD = sc.parallelize(names,2);
		
		JavaRDD<String> sample = nameRDD.sample(false, 0.5);
		
		List<Tuple2<Integer, String>> skewedKeys = sample.mapToPair(new PairFunction<String, String,Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t,2);
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
		}).mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
				 
				return new Tuple2<Integer, String>(tuple._2,tuple._1);
			}
		}).sortByKey(false).take(1);
		
		
		Tuple2<Integer, String> tuple2 = skewedKeys.get(0);
		String skewedKey = tuple2._2;
		
		final Broadcast<String> broadcastSkewed = sc.broadcast(skewedKey);
		
		nameRDD.filter(new Function<String, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				return !broadcastSkewed.value().equals(v1);
			}
		}).foreach(new VoidFunction<String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				 System.out.println("filtered result:"+t);
			}
		});;
		
	}
}
