package main.java.operator;

import java.util.Arrays;

//import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * WordCount
 * @author root
 *
 */
public class WC {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
				.setMaster("local")
				.setAppName("Word哥");
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> linesRDD = sc.textFile("cs");
		
		JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String str) throws Exception {
				return Arrays.asList(str.split(" "));
			}
		});
		
		/**
		 * 在scala api中将rdd<String>  RDD<String,Integer>   使用map算子就可以完成
		 * 但是呢？在java里面需要使用mapToPair
 		 */
		
		JavaPairRDD<String, Integer> pairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String str) throws Exception {
				return new Tuple2<String, Integer>(str, 1);
			}
		});
		
		JavaPairRDD<String, Integer> resultRdd = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		resultRdd.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> tuple2) throws Exception {
				 System.out.println(tuple2);
			}
		});
		
		
	}
}
