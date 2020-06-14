package main.java.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class SampleTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("xiaopangzi").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> rdd = context.textFile("log.txt");
		rdd = rdd.cache();
		JavaRDD<String> sampleRDD = rdd.sample(false, 0.9);
		
		final String key1 = sampleRDD.mapToPair(new PairFunction<String, String,Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				String key = t.split("\t")[1];
				return new Tuple2<String, Integer>(key,1);
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
		}).mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Integer, String>(tuple._2,tuple._1);
			}
		}).sortByKey(false).first()._2;
		
		System.out.println("key:" + key1);
		
		final Broadcast<String> broadcastKey = context.broadcast(key1);
		
		
		rdd.filter(new Function<String, Boolean>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				String key = broadcastKey.value();
				return !v1.contains(key);
			}
		}).foreach(new VoidFunction<String>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
		 
		context.close();
		
		
	}
}
