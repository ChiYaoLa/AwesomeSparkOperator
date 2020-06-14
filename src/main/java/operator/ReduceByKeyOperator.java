package main.java.operator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

// reduceByKey = groupByKey + reduce
// shuffle 洗牌  = map端 + reduce端
// spark里面这个reduceByKey在map端自带Combiner

public class ReduceByKeyOperator {

	public static void main(String[] args) {
		System.out.println((int)'c');
		
		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster(
				"local");
		conf.set("spark.default.parallelism", "50");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Tuple2<String,Integer>> scoreList = Arrays.asList(
				new Tuple2<String, Integer>("a" , 150),
				new Tuple2<String, Integer>("c" , 100),
				new Tuple2<String, Integer>("a" , 100),
				new Tuple2<String, Integer>("b" , 80));
		
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(scoreList);
		
		System.out.println("rdd.partitions.size()"+rdd.partitions().size());
		rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		}).mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String,Integer>>, Iterator<Tuple2<String,Integer>>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, Integer>> call(Integer v1, Iterator<Tuple2<String, Integer>> v2) throws Exception {
				System.out.println("partitionId:"+v1);
				while(v2.hasNext()){
					Tuple2<String, Integer> next = v2.next();
					System.out.println(next._1);
				}
				return null;
			}
		}, false).foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println("name ： " + tuple._1 + " score :" + tuple._2);
			}
		});
		
		while(true){}
		
//		sc.close();
	}
}
