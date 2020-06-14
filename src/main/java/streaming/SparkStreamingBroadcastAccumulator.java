package main.java.streaming;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkStreamingBroadcastAccumulator {
	//广播变量
	private static volatile Broadcast<List<String>> broadcastList = null;
	//计数器
	private static volatile Accumulator<Integer> accumulator = null;
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCountOnline");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		/**
		 * 使用Broadcast广播黑名单到每个Executor中
		 */
		broadcastList = jsc.sparkContext().broadcast(Arrays.asList("Hadoop","Mahout","Hive"));
		/**
		 * 全局计数器，用于统计在线过滤了多少个黑名单
		 */
		accumulator = jsc.sparkContext().accumulator(0,"OnlineBlacklistCounter");
		
		JavaReceiverInputDStream<String> lines = jsc.socketTextStream("192.168.5.128", 9999);
	 

		JavaPairDStream<String, Integer> ones = lines.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairDStream<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
	 
		counts.foreachRDD(new Function2<JavaPairRDD<String, Integer>,Time,Void>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaPairRDD<String, Integer> rdd, Time arg1) throws Exception {
				rdd.filter(new Function<Tuple2<String,Integer>, Boolean>() {
					
					@Override
					public Boolean call(Tuple2<String, Integer> wordPair) throws Exception {
						if(broadcastList.value().contains(wordPair._1)){
							accumulator.add(wordPair._2);
							return false;
							}
						else
							return true;
					}
				}).collect();
				System.out.println(broadcastList.value().toString()+":"+accumulator.value()+" times");
				return null;
			}			
		}); 
 		
 		jsc.start();
 		jsc.awaitTermination();
 		jsc.close();
	}
}
