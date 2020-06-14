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
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		
		/**
		 * local[*]模式下并行度的设置  textFile方法里面设置
		 *   def defaultMinPartitions: Int = math.min(defaultParallelism, 2)
		 *  如果想将application程序提交到集群，可以设置master为spark://ip:7077
		 *  
		 *  
		 *  并行度如何设置呢？
		 *       一般讲并行度设置为cores的2-3倍，为什么不是1倍的关系？
		 *   
		 */
		
		SparkConf conf = new SparkConf().setAppName("WordCount")
				.setMaster("local[3]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("C:\\Users\\zfg\\Desktop\\log.txt",10);
		
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
		});
		
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String,Integer>(word,1);
			}
		});
		
		JavaPairRDD<String, Integer> results = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				return a+b;
			}
		});
		
		JavaPairRDD<Integer, String> mapToPair = results.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer,String>() {

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> results) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(results._2,results._1);
			}
		});
		
		JavaPairRDD<Integer, String> sortByKey = mapToPair.sortByKey();
			 
		
		for (Tuple2<Integer, String> tuple : sortByKey.collect()) {
			System.out.println(" count: " + tuple._1 + " word : " + tuple._2);
		}
		
		sc.close();
	}
}
