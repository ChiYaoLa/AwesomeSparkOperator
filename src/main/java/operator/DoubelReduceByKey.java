package main.java.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.util.Random;

import java.util.Arrays;
import java.util.List;

public class DoubelReduceByKey {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("DoubelReduceByKey")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		
		List<Tuple2<String, Integer>> dataList = Arrays.asList(
				new Tuple2<String, Integer>("hello", 1),
				new Tuple2<String, Integer>("hello", 1),
				new Tuple2<String, Integer>("hello", 1),
				new Tuple2<String, Integer>("hello", 1)
				);
		
		JavaPairRDD<String, Integer> dataRDD = sc.parallelizePairs(dataList);
		dataRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
				 Random random = new Random();
				 int prefix = random.nextInt(10);
				 return new Tuple2<String, Integer>(prefix+"_"+tuple._1,tuple._2);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 +v2;
			}
		}, 4).mapToPair(new PairFunction<Tuple2<String,Integer>, String	, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
				return new Tuple2<String, Integer>(tuple._1.split("_")[1],tuple._2);
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
		}).foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println("word:" + tuple._1 + "\tcount:" + tuple._2);
			}
		});;
		
		
		
	}
}
