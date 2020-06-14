package main.java.operator;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SaprkApp {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("wangpangzi").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> rdd = context.textFile("log.txt");
		
		JavaRDD<String> sampleRdd = rdd.sample(true,0.5);
		
		JavaPairRDD<String, String> mapRdd = sampleRdd.mapToPair(new PairFunction<String, String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String line) throws Exception {
				String[] split = line.split("\t");
				return new Tuple2<>(split[1],line);
			}
		});
	 
		//每组数据
		JavaPairRDD<String, Iterable<String>> groupRdd = mapRdd.groupByKey();
		
		JavaPairRDD<Integer, String> mapToPair = groupRdd.mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, Integer, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
				Iterable<String> _2 = tuple._2;
				Iterator<String> iterator = _2.iterator();
				int i = 0 ;
				while(iterator.hasNext()){
					iterator.next();
					i++;
				}
				Tuple2<Integer, String> tuple2 = new Tuple2<>(i, tuple._1);
				return tuple2;
			}
		});
		
		JavaPairRDD<Integer, String> sortByKey = mapToPair.sortByKey(false);
		List<Tuple2<Integer,String>> list = sortByKey.take(1);
		Tuple2<Integer, String> tuple2 = list.get(0);
		System.out.println(list.get(0));
		final String filterS = tuple2._2;
		JavaRDD<String> javaRDD = rdd.filter(new Function<String, Boolean>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String arg0) throws Exception {
				if(arg0.contains(filterS)){
					return false;
				}else {
					return true;
				}
			}
		});
		javaRDD.foreach(new VoidFunction<String>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String arg0) throws Exception {
				System.out.println(arg0);
			}
		});
		
		context.close();
		
	}
}
