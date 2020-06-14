package main.java.streaming;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 基于滑动窗口的热点搜索词实时统计
 * @author Administrator
 *
 */
public class WindowOperator {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("WindowHotWord");  
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
   		jssc.checkpoint("hdfs://192.168.126.111:9000/sscheckpoint02");
		
		JavaReceiverInputDStream<String> searchLogsDStream = jssc.socketTextStream("hadoop1", 9999);
		
		//word	1
		
		JavaDStream<String> searchWordsDStream = searchLogsDStream.flatMap(new FlatMapFunction<String, String>() {
  
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" "));
			}
		}); 
		
		// 将搜索词映射为(searchWord, 1)的tuple格式
		JavaPairDStream<String, Integer> searchWordPairDStream = searchWordsDStream.mapToPair(
				
				new PairFunction<String, String, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String searchWord)
							throws Exception {
						return new Tuple2<String, Integer>(searchWord, 1);
					}
					
				});
	 
		/**
		 * 每隔10秒，计算最近60秒内的数据，那么这个窗口大小就是60秒，里面有12个rdd，在没有计算之前，这些rdd是不会进行计算的。那么在计算的时候会将这12个rdd聚合起来，然后一起执行reduceByKeyAndWindow
		 * 操作 ，reduceByKeyAndWindow是针对窗口操作的而不是针对DStream操作的。
		 */
	   	/*    JavaPairDStream<String, Integer> searchWordCountsDStream = 
				
				searchWordPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
		}, Durations.seconds(60), Durations.seconds(10));*/        
		
		 	  JavaPairDStream<String, Integer> searchWordCountsDStream = 
				
				 searchWordPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
					
				},new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 - v2;
					}
					
				}, Durations.seconds(60), Durations.seconds(10));      
	  	searchWordCountsDStream.print();
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}

}
