package main.java.streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
/**
 * 1、local的模拟线程数必须大于等于2 因为一条线程被receiver(接受数据的线程)占用，另外一个线程是job执行
 * 2、Durations时间的设置，就是我们能接受的延迟度，这个我们需要根据集群的资源情况以及监控，ganglia  每一个job的执行时间
 * 3、 创建JavaStreamingContext有两种方式 （sparkconf、sparkcontext）
 * 4、业务逻辑完成后，需要有一个output operator
 * 5、JavaStreamingContext.start()
 * 6、JavaStreamingContext.stop()无参的stop方法会将sparkContext一同关闭，stop(false)
 * 7、JavaStreamingContext.stop() 停止之后是不能在调用start   
 * 8、JavaStreamingContext.start() straming框架启动之后是不能在次添加业务逻辑
 */

public class WordCountOnTransform {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		 
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCountOnline");
		/**
		 * 在创建streaminContext的时候 设置batch Interval
		 */
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		JavaReceiverInputDStream<String> lines = jsc.socketTextStream("hadoop1", 9999);
		
		JavaPairDStream<String, Integer> resultDStram = lines.transformToPair(new Function<JavaRDD<String>, JavaPairRDD<String,Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public JavaPairRDD<String, Integer> call(JavaRDD<String> rdd) throws Exception {
				return rdd.flatMap(new FlatMapFunction<String, String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<String> call(String line) throws Exception {
						 
						return Arrays.asList(line.split(" "));
					}
				}).mapToPair(new PairFunction<String, String, Integer>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String val) throws Exception {
						return new Tuple2<String, Integer>(val,1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						 
						return v1+v2;
					}
				});
			}
		});
		
		/**
		 * 使用foreachRDD这个算子，必须对抽取出来的RDD执行action类算子的触发
		 */
		resultDStram.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
				rdd.foreach(new VoidFunction<Tuple2<String,Integer>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Tuple2<String, Integer> tuple) throws Exception {
						 System.out.println(tuple);
					}
				});
			}
		});
 		jsc.start();
 		jsc.awaitTermination();
 		jsc.stop(false);
	}
	
}
