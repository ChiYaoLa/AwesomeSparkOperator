package main.java.broadcast;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("WordCount")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		final Accumulator<Integer> linesCountAccumulator = sc.accumulator(0);
		
		JavaRDD<String> lines = sc.textFile("F:\\JAVA\\JavaWorkSpace\\testmaven\\src\\main\\resources\\userLog",10);
		
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;


			public Iterable<String> call(String line) throws Exception {
				linesCountAccumulator.add(1);
				return Arrays.asList(line.split("\t"));
			}
		});
		
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String,Integer>(word,1);
			}
		});
		
		JavaPairRDD<String, Integer> results = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				return a+b;
			}
		});
		
		List<Tuple2<Integer, String>> topN = results.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
				return new Tuple2<Integer, String>(tuple._2,tuple._1);
			}
		}).sortByKey(false).take(3);
		
		List<Tuple2<String, Integer>> collect = results.collect();
		for (Tuple2<String, Integer> tuple2 : collect) {
			System.out.println(tuple2);
		}
		
		
		for(Tuple2<Integer, String> tuple2 : topN){
			System.out.println("Word:" + tuple2._2 + "\tCount:"+tuple2._1);
		}

		System.out.println("一共有" + linesCountAccumulator.value());
		sc.close();
	}
}
