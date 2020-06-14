package main.java.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.google.common.base.Optional;

import scala.Tuple2;

public class JoinOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JoinOperator")
				.setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 模拟集合
		List<Tuple2<Integer, String>> nameList = Arrays.asList(
				new Tuple2<Integer,String>(1, "xuruyun"),
				new Tuple2<Integer,String>(1, "angelbaby"),
				new Tuple2<Integer,String>(2, "liangyongqi"),
				new Tuple2<Integer,String>(3, "wangfei"));
		
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer,Integer>(1, 150),
				new Tuple2<Integer,Integer>(2, 100),
				new Tuple2<Integer,Integer>(12, 100)
				 );
		
		JavaPairRDD<Integer,String> nameRDD = sc.parallelizePairs(nameList,3);
		JavaPairRDD<Integer,Integer> scoreRDD = sc.parallelizePairs(scoreList,2);
		
		scoreRDD.rightOuterJoin(nameRDD);
		 
		JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullOuterJoinRDD = scoreRDD.fullOuterJoin(nameRDD);
		System.out.println(fullOuterJoinRDD.count());
		
		sc.close();
	}
}
