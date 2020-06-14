package main.java.optimization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.netlib.util.booleanW;

import scala.Tuple2;

/**
 * 使用广播变量实现join的功能
 * @author root
 *
 */
public class JoinBroadCastOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JoinBroadCastOperator")
				.setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 模拟集合
		List<Tuple2<Integer, String>> nameList = Arrays.asList(
				new Tuple2<Integer,String>(1, "xuruyun"),
				new Tuple2<Integer,String>(2, "liangyongqi"),
				new Tuple2<Integer,String>(3, "wangfei"));
		
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer,Integer>(1, 150),
				new Tuple2<Integer,Integer>(2, 100),
				new Tuple2<Integer,Integer>(1, 150),
				new Tuple2<Integer,Integer>(2, 100)
				 );
		
		final int count = 0;
		/**
		 * join
		 * <Integer,Tuple2<String,Interger>>
		 */
		
		JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(nameList);
		final Broadcast<List<Tuple2<Integer, Integer>>> scoreBroadCast = sc.broadcast(scoreList);
		
		 
		JavaPairRDD<Integer, Tuple2<String, Integer>> broadCastResult = nameRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,String>, Integer, Tuple2<String, Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<Integer, Tuple2<String, Integer>>> call(Tuple2<Integer, String> tuple) throws Exception {
				List<Tuple2<Integer, Tuple2<String, Integer>>>  list = new ArrayList<>();
				List<Tuple2<Integer, Integer>> scoreList = scoreBroadCast.value();
				for (Tuple2<Integer, Integer> tuple2 : scoreList) {
					System.out.println(count);
					Integer id = tuple2._1;
					Integer score  = tuple2._2;
					Integer nameId = tuple._1;
					String name = tuple._2;
					if(id.intValue() == nameId.intValue()){
						list.add(new Tuple2<Integer, Tuple2<String,Integer>>(id, new Tuple2<String, Integer>(name, score)));
					}
				}
				return list;
			}
		});
		
		List<Tuple2<Integer, Tuple2<String, Integer>>> collect = broadCastResult.collect();
		for (Tuple2<Integer, Tuple2<String, Integer>> tuple2 : collect) {
			System.out.println(tuple2);
		}
		
		 
		
		sc.close();
	}
}











