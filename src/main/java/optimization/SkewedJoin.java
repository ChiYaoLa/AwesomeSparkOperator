package main.java.optimization;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import breeze.stats.distributions.Rand;
import scala.Tuple2;

/**
 * 采样倾斜key并分拆join操作
 * @author root
 *
 */
public class SkewedJoin {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
				.setMaster("local")
				.setAppName("SkewedJoin");
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		List<Tuple2<String, Integer>> nameList = new ArrayList<>();
		nameList.add(new Tuple2<String, Integer>("bjsxt", 1));
		nameList.add(new Tuple2<String, Integer>("bjsxt", 1));
		nameList.add(new Tuple2<String, Integer>("shsxt", 1));
		nameList.add(new Tuple2<String, Integer>("gzsxt", 1));
		
		List<Tuple2<String, Integer>> scoreList = new ArrayList<>();
		scoreList.add(new Tuple2<String, Integer>("bjsxt", 100));
		scoreList.add(new Tuple2<String, Integer>("shsxt", 95));
		scoreList.add(new Tuple2<String, Integer>("gzsxt", 90));
		
		
		JavaPairRDD<String, Integer> nameRDD = sc.parallelizePairs(nameList);
		JavaPairRDD<String, Integer> scoreRDD = sc.parallelizePairs(scoreList);
		
		/**
		 *  因为nameRDD有导致数据倾斜的key，但是不知道是哪些key，要采样
		 */
		JavaPairRDD<String, Integer> sample = nameRDD.sample(false, 0.5);
		String skewedKey = sample.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		}).mapToPair(new PairFunction<Tuple2<String,Integer>, Integer,String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
				return new Tuple2<Integer, String>(tuple._2, tuple._1);
			}
		}).sortByKey(false).take(1).get(0)._2;
		/**
		 *j将导致数据倾斜的key放入广播变量
		 */
		final Broadcast<String> skewedKeyBroadcast = sc.broadcast("bjsxt");
		
		/**
		 * 找到导致数据倾斜的key，拆分 nameRDD和scoreRDD
		 */
		JavaPairRDD<String, Integer> skewedNameRDD = nameRDD.filter(new Function<Tuple2<String,Integer>, Boolean>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				String skewedKey = skewedKeyBroadcast.value();
				return skewedKey.equals(v1._1);
			}
		});
		
		JavaPairRDD<String, Integer> commonNameRDD = nameRDD.filter(new Function<Tuple2<String,Integer>, Boolean>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				String skewedKey = skewedKeyBroadcast.value();
				return !skewedKey.equals(v1._1);
			}
		});
		
		JavaPairRDD<String, Integer> skewedScoreRDD = scoreRDD.filter(new Function<Tuple2<String,Integer>, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				String skewedKey = skewedKeyBroadcast.value();
				return skewedKey.equals(v1._1);
			}
		});
		
		JavaPairRDD<String, Integer> commonScoreRDD = scoreRDD.filter(new Function<Tuple2<String,Integer>, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				String skewedKey = skewedKeyBroadcast.value();
				return !skewedKey.equals(v1._1);
			}
		});
		
		
		/**
		 * skewedNameRDD打上n以内的随机数
		 * skewedScoreRDD膨胀n倍
		 */
		JavaPairRDD<String, Integer> prefixSkewedNameRDD = skewedNameRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
				 Random random = new Random();
				 int prefix = random.nextInt(2);
				 return new Tuple2<String, Integer>(prefix+ "_" + tuple._1,tuple._2);
			}
		});
		
		JavaPairRDD<String, Integer> expandScoreRDD = skewedScoreRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Integer>, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Integer> tuple) throws Exception {
				List<Tuple2<String, Integer>> list = new ArrayList<>();
				for (int i = 0; i < 2; i++) {
					list.add(new Tuple2<String, Integer>(i + "_" + tuple._1, tuple._2));
				}
				return list;
			}
		});
		
		/**
		 * 现在已经随机和膨胀完了，join
		 */
		JavaPairRDD<String, Tuple2<Integer, Integer>> prefixJoin1 = prefixSkewedNameRDD.join(expandScoreRDD,2);
		  JavaPairRDD<String, Tuple2<Integer, Integer>> join1 = prefixJoin1.mapToPair(new PairFunction<Tuple2<String,Tuple2<Integer,Integer>>, String, Tuple2<Integer,Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Tuple2<Integer, Integer>> call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
				return new Tuple2<String ,Tuple2<Integer,Integer>>(tuple._1.split("_")[1], tuple._2);
			}
		});

			 
		
		
		JavaPairRDD<String, Tuple2<Integer, Integer>> join2 = commonNameRDD.join(commonScoreRDD);
		
		JavaPairRDD<String, Tuple2<Integer, Integer>> result = join1.union(join2);
		
		List<Tuple2<String, Tuple2<Integer, Integer>>> collect = result.collect();
		for (Tuple2<String, Tuple2<Integer, Integer>> tuple2 : collect) {
			System.out.println(tuple2);
		}
		
			
	}
}
