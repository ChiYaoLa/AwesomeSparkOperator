package main.java.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.util.Random;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DoubleJoin {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("DoubelReduceByKey")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Tuple2<String, Integer>> dataList1 = Arrays.asList(
				new Tuple2<String, Integer>("hello",1),
				new Tuple2<String, Integer>("hello",2),
				new Tuple2<String, Integer>("bjsxt",3),
				new Tuple2<String, Integer>("shsxt",1),
				new Tuple2<String, Integer>("gzsxt",1)
				);
		List<Tuple2<String, Integer>> dataList2 = Arrays.asList(
				new Tuple2<String, Integer>("hello",100),
				new Tuple2<String, Integer>("bjsxt",99),
				new Tuple2<String, Integer>("shsxt",88),
				new Tuple2<String, Integer>("gzsxt",66)
				);
		JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(dataList1);
		JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(dataList2);
		JavaPairRDD<String, Integer> sample = rdd1.sample(false, 1);
		
		 String skewedKey = sample.map(new Function<Tuple2<String,Integer>, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, Integer> tuple2) throws Exception {
				// TODO Auto-generated method stub
				return tuple2._1;
			}
		}).mapToPair(new PairFunction<String, String,Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t,1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				 
				return v1 + v2;
			}
		}).mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
				 
				return new Tuple2<Integer, String>(tuple._2,tuple._1);
			}
		}).sortByKey(false).take(1).get(0)._2;
		
		 /**
		  * 将导致倾斜的key放入到广播变量中
		  */
		 
		 final Broadcast<String> broadcastSkewedKey = sc.broadcast(skewedKey);
		 /**
		 * 将RDD1拆分成RDD1_1(导致倾斜的key) RDD1_2
		 */
		JavaPairRDD<String, Integer> skewedRDD1 = rdd1.filter(new Function<Tuple2<String,Integer>, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				 
				return broadcastSkewedKey.value().equals(v1._1);
			}
		});
		
		JavaPairRDD<String, Integer> commonRDD1 = rdd1.filter(new Function<Tuple2<String,Integer>, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				 
				return !broadcastSkewedKey.value().equals(v1._1);
			}
		});
		
		
		
		
		JavaPairRDD<String, Integer> skewedRDD2 = rdd2.filter(new Function<Tuple2<String,Integer>, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				 
				return broadcastSkewedKey.value().equals(v1._1);
			}
		});
		
		JavaPairRDD<String, Integer> commonRDD2 = rdd2.filter(new Function<Tuple2<String,Integer>, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				 
				return !broadcastSkewedKey.value().equals(v1._1);
			}
		});
		
		
		JavaPairRDD<String, Integer> prefixSkewedRDD1 = skewedRDD1.mapToPair(new PairFunction<Tuple2<String,Integer>, String,Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
				Random random = new Random();
				int prefix = random.nextInt(2);
				return new Tuple2<String, Integer>(prefix+ "_"+tuple._1,tuple._2);
			}
		});
		
		JavaPairRDD<String, Integer> expandSkewedRDD2 = skewedRDD2.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Integer>, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Integer> t) throws Exception {
				List<Tuple2<String, Integer>> list = new ArrayList<>();
				for (int i = 0; i < 2; i++) {
					list.add(new Tuple2<String, Integer>(i+"_"+t._1,t._2));
				}
				 return list;
			}
		});
		
		JavaPairRDD<String, Tuple2<Integer, Integer>> resultRDD1 = prefixSkewedRDD1.join(expandSkewedRDD2,2).mapToPair(new PairFunction<Tuple2<String,Tuple2<Integer,Integer>>, String, Tuple2<Integer, Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Tuple2<Integer, Integer>> call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
				return new Tuple2<String, Tuple2<Integer, Integer>>(tuple._1.split("_")[1],tuple._2);
			}
		});
		
		JavaPairRDD<String, Tuple2<Integer, Integer>> resultRDD2 = commonRDD1.join(commonRDD2,3);
		
		JavaPairRDD<String, Tuple2<Integer, Integer>> union = resultRDD1.union(resultRDD2);
		union.foreach(new VoidFunction<Tuple2<String,Tuple2<Integer,Integer>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Tuple2<Integer, Integer>> t) throws Exception {
				 System.out.println(t);
				
			}
		});
		
		sc.stop();
	}
}
