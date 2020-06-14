package main.java.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;



/**
 * 并行度问题：
 * 1、linesDStram里面封装到的是RDD， RDD里面有partition与这个topic的parititon数是一致的。
 * 2、从kafka中读来的数据封装一个DStram里面，可以对这个DStream重分区 reaprtitions(numpartition)
 * @author root
 *
 */
public class SparkStreamingOnKafkaDirected {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().
				setAppName("SparkStreamingOnKafkaDirected")
				.setMaster("local[1]");
		
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
		
		
		Map<String, String> kafkaParameters = new HashMap<String, String>();
		kafkaParameters.put("metadata.broker.list", "192.168.126.111:9092,192.168.126.112:9092,192.168.126.113:9092");
		
		HashSet<String> topics = new HashSet<String>();
		topics.add("Abgelababy");
		JavaPairInputDStream<String,String> lines = KafkaUtils.createDirectStream(jsc,
				String.class, 
				String.class,
				StringDecoder.class,
				StringDecoder.class,
				kafkaParameters,
				topics);
		
	/*	lines.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, String> rdd) throws Exception {
			 System.out.println("rdd.partitions().size()"+rdd.partitions().size());
			}
			
		});*/
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() { //如果是Scala，由于SAM转换，所以可以写成val words = lines.flatMap { line => line.split(" ")}
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(Tuple2<String,String> tuple) throws Exception {
				return Arrays.asList(tuple._2.split("\t"));
			}
		});
		
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		
		JavaPairDStream<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() { //对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		/**
		 * foreachRDD  output operator
		 * transform transformation
		 */
		/*wordsCount.transformToPair(new Function<JavaPairRDD<String,Integer>, JavaPairRDD<String,Integer>>() {
			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> rdd) throws Exception {
				return rdd.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer,String>() {

					*//**
					 * 
					 *//*
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
						return new Tuple2<Integer, String>(tuple._2,tuple._1);
					}
				}).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer,String>, String,Integer>() {

					*//**
					 * 
					 *//*
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String,Integer> call(Tuple2<Integer,String> tuple) throws Exception {
						return new Tuple2<String,Integer>(tuple._2,tuple._1);
					}
				});
			}
		}).print();*/
		
		wordsCount.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
				List<Tuple2<String, Integer>> collect = rdd.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer,String>() {
 
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
						return new Tuple2<Integer, String>(tuple._2,tuple._1);
					}
				}).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer,String>, String,Integer>() {

					 
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String,Integer> call(Tuple2<Integer,String> tuple) throws Exception {
						return new Tuple2<String,Integer>(tuple._2,tuple._1);
					}
				}).collect();
				for (Tuple2<String, Integer> tuple : collect) {
					System.out.println(tuple);
				}
			}
		});
		
		
		jsc.start();
		
		jsc.awaitTermination();
		jsc.close();

	}

}
