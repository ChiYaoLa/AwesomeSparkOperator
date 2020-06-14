package main.java.streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

import scala.Tuple2;


/**
 *
 *  Spark standalone or Mesos with cluster deploy mode only:
 *  在提交application的时候  添加 --supervise 选项  如果Driver挂掉 会自动启动一个Driver
 *  	SparkStreaming
 * @author root
 *
 */
public class SparkStreamingOnHDFS {
	public static void main(String[] args) {
		final SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("SparkStreamingOnHDFS");
		
		// JavaStreamingContext jsc = new JavaStreamingContext(conf,
		// Durations.seconds(5));
		
		final String checkpointDirectory = "hdfs://hadoop1:9000/library/SparkStreaming/CheckPoint_20170523";
		
		JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
			@Override
			public JavaStreamingContext create() {  
				return createContext(checkpointDirectory,conf);
			}
		};
		 
		JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpointDirectory, factory);
		jsc.start();
		jsc.awaitTermination();
		// jsc.close();
	}

	@SuppressWarnings("deprecation")
	private static JavaStreamingContext createContext(String checkpointDirectory,SparkConf conf) {

		// If you do not see this printed, that means the StreamingContext has
		// been loaded
		// from the new checkpoint
		System.out.println("Creating new context");
		SparkConf sparkConf = conf;
		// Create the context with a 1 second batch size

		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(15));
		
		ssc.checkpoint(checkpointDirectory);
		
		/**
		 * 只是监控文件夹下新增的文件，减少的文件是监控不到的  文件内容有改动也是监控不到
		 */
		JavaDStream<String> lines = ssc.textFileStream("hdfs://hadoop1:9000/hdfs/");
		 
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(s.split(" "));
			}
		});
		

		JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
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
	 
		counts.print();
		return ssc;
	}

}
