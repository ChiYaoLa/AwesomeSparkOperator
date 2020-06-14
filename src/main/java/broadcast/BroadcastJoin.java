package main.java.broadcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 使用广播变量来模拟join
 *  适应的场景是：一个RDD比较大，一个RDD比较小
 *  小的RDD占用内存量小于executor memory * (90%*60%)
 * @author root
 *	
 */
public class BroadcastJoin {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
				.setMaster("local")
				.setAppName("BroadCastOperator");
		final JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		// 模拟集合
		List<Tuple2<Integer, String>> nameList = Arrays.asList(
				new Tuple2<Integer,String>(1, "xuruyun"),
				new Tuple2<Integer,String>(2, "liangyongqi"),
				new Tuple2<Integer,String>(3, "wangfei"));
		
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer,Integer>(1, 150),
				new Tuple2<Integer,Integer>(2, 100)
				 );
		JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(nameList);
		JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scoreList);
        
		List<Tuple2<Integer, String>> names = nameRDD.collect();
		Map<Integer,String > nameMap = new HashMap<Integer,String>();
		for (Tuple2<Integer, String> tuple2 : names) {
			nameMap.put(tuple2._1, tuple2._2);
		}
		/**
		 * nameRDD.join(scoreRDD)
		 */
		final Broadcast<Map<Integer, String>> broadcast = sc.broadcast(nameMap);
		
		List<Tuple2<Integer, Tuple2<String, Integer>>> collect = scoreRDD.mapToPair(new PairFunction<Tuple2<Integer,Integer>, Integer, Tuple2<String, Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;


			public Tuple2<Integer, Tuple2<String, Integer>> call(Tuple2<Integer, Integer> tuple) throws Exception {
				Map<Integer, String> nameMap = broadcast.value();
				Integer id = tuple._1;
				String name = nameMap.get(tuple._1);
				Integer score = tuple._2;
				return new Tuple2<Integer, Tuple2<String, Integer>>(id,new Tuple2<String,Integer>(name,score));
			}
		}).collect();
		
		for (Tuple2<Integer, Tuple2<String, Integer>> tuple2 : collect) {
			System.out.println(tuple2);
		}
		sc.stop();
		
	}
}
