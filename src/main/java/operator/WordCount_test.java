package main.java.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/*
 * wordcount示例
 */
public class WordCount_test {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("WordCount")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> linesRdd = sc.textFile("cs",100);
		
		System.out.println(linesRdd.partitions().size());
	
		JavaRDD<String> wordsRdd = linesRdd.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String t) throws Exception {
				return Arrays.asList(t.split("\t"));
			}
		});
		
		JavaPairRDD<String, Integer> pairs = wordsRdd.mapToPair(new PairFunction<String, String,Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		});
		 
		JavaPairRDD<String, Integer> reduceRdd = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
/*		JavaPairRDD<Integer, String> mapToPair = reduceRdd.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> v) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Integer, String>(v._2, v._1);
			}
		});*/
		
//		JavaPairRDD<Integer, String> sortByKey = mapToPair.sortByKey();
		
		 JavaPairRDD<Integer, Tuple2<String, Integer>> keyBy = reduceRdd.keyBy(new Function<Tuple2<String,Integer>, Integer>() {

			@Override
			public Integer call(Tuple2<String, Integer> v1) throws Exception {
				
				return v1._2;
			}
		});
		
		
		
		
		 List<Tuple2<Integer,Tuple2<String,Integer>>> collect2 = keyBy.collect();
		 for (Tuple2<Integer, Tuple2<String, Integer>> tuple2 : collect2) {
			System.out.println("key:"+tuple2._1+"  value:"+tuple2._2);
		}
				
		 
		 
		 
		
 	/*JavaPairRDD<Integer, Tuple2<String, Integer>> sortByKey = keyBy.sortByKey();
		
		JavaRDD<Tuple2<String, Integer>> map = sortByKey.map(new Function<Tuple2<Integer,Tuple2<String,Integer>>, Tuple2<String,Integer>>() {

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String,Integer> call(Tuple2<Integer, Tuple2<String, Integer>> v1) throws Exception {
				// TODO Auto-generated method stub
				
				return v1._2;
			}
		}); 
		
		
	 	List<Tuple2<String, Integer>> collect = map.collect();
		 for (Tuple2<String, Integer> tuple2 : collect) {
			System.out.println(tuple2._1+"==="+tuple2._2);
		} */
		
		
		/*reduceRdd.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1+"---"+t._2);
			}
		});*/
		 
		 sc.stop();
	}
}
