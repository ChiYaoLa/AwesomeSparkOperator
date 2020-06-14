package main.java.streaming.test;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class WordCountOnLine {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("WordCountOnLine");
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));
		
		JavaReceiverInputDStream<String> linesDStram = streamingContext.socketTextStream("hadoop1", 9999);
		
		JavaDStream<String> wordsDStream = linesDStram.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String str) throws Exception {
				return Arrays.asList(str.split(" "));
			}
		});
		
		
		JavaPairDStream<String, Integer> pairDStram = wordsDStream.mapToPair(new PairFunction<String, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t,1);
			}
		});
		
		JavaPairDStream<String, Integer> resultDStream = pairDStram.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		resultDStream.print();
		
		streamingContext.start();
		streamingContext.awaitTermination();
		
		streamingContext.close();
	}
}
