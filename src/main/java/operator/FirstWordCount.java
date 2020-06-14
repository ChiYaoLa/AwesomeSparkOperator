package main.java.operator;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class FirstWordCount {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
			.setAppName("WC")
			.setMaster("local");
		
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		JavaRDD<String> lineRDD = jsc.textFile("cs");
		
		JavaRDD<String> wordsRDD = lineRDD.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
		});


		JavaPairRDD<String, Integer> pairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				 
				return new Tuple2<String, Integer>(word,1);
			}
		});
		
		JavaPairRDD<String, Integer> reduceByKeyRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		reduceByKeyRDD.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println("word："+tuple._1+"\tcount:"+tuple._2);
			}
		});
		
		jsc.stop();
		
	}
}
