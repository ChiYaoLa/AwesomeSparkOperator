package main.java.optimization;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

// reduceByKey = groupByKey + reduce
// shuffle 洗牌  = map端 + reduce端
// spark里面这个reduceByKey在map端自带Combiner

public class ReduceByKeyOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster(
				"local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Tuple2<String,Integer>> scoreList = Arrays.asList(
				new Tuple2<String, Integer>("bjsxt" , 150),
				new Tuple2<String, Integer>("bjsxt" , 100),
				new Tuple2<String, Integer>("bjsxt" , 100),
				new Tuple2<String, Integer>("bjsxt" , 100),
				new Tuple2<String, Integer>("bjsxt" , 100),
				new Tuple2<String, Integer>("bjsxt" , 100),
				new Tuple2<String, Integer>("bjsxt" , 100),
				
				new Tuple2<String, Integer>("bjsxt" , 100),
				new Tuple2<String, Integer>("bjsxt" , 80));
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(scoreList);
		
		/**
		 * 对每一个key添加随机前缀
		 */
		 JavaPairRDD<String, Integer> prefixRDD = rdd.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
				 Random random = new Random();
				 int prefix = random.nextInt(2);
				return new Tuple2<String, Integer>(prefix+"_"+tuple._1, tuple._2);
			}
		});
		 /**
		  * 进行局部聚合
		  */
		 JavaPairRDD<String, Integer> tmpReduceRDD = prefixRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				 
				return v1 + v2;
			}
		},2);
		 
		 tmpReduceRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String,Integer>>, Iterator<Tuple2<String,Integer>>>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public Iterator<Tuple2<String,Integer>> call(Integer v1, Iterator<Tuple2<String, Integer>> v2) throws Exception {
					 int partitionId = v1;
					 while (v2.hasNext()) {
						 Tuple2<String, Integer> next = v2.next();
						 String name  = next._1;
						 int id  = next._2;
						 System.out.println("partitionId:"+partitionId+"="+name+"_"+id);
					}
					 
					return Arrays.asList(new Tuple2<String,Integer>("as", 1)).iterator();
				}
			}, false).collect();
		 
		 /**
		  * 去掉随机前缀
		  */
		 JavaPairRDD<String, Integer> removePrefixRDD = tmpReduceRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
				return new Tuple2<String, Integer>(tuple._1.split("_")[1], tuple._2);
			}
		});
		 
		 /**
		  * 全局聚合
		  */
		 JavaPairRDD<String, Integer> result = removePrefixRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		},2);
		 
		
	}
}
