package main.java.operator;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;


public class AggregateByKeyOperator {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("AggregateOperator")
				.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		List<Tuple2<Integer,Integer>> dataList =  new ArrayList<Tuple2<Integer,Integer>>();
		dataList.add(new Tuple2<Integer, Integer>(1,99));
		dataList.add(new Tuple2<Integer, Integer>(2,78));
		dataList.add(new Tuple2<Integer, Integer>(1,89));
		dataList.add(new Tuple2<Integer, Integer>(2,3));
		dataList.add(new Tuple2<Integer, Integer>(3,3));
		dataList.add(new Tuple2<Integer, Integer>(3,30));
		
		JavaPairRDD<Integer, Integer> dataRdd = sc.parallelizePairs(dataList,2);
		System.out.println("dataRdd.partitions().size():"+dataRdd.partitions().size());
		
		JavaPairRDD<Integer, Integer> aggregateByKey = dataRdd.aggregateByKey(80, new Function2<Integer, Integer, Integer>() {
			/**
			 * aaa
			 * 
			 *  seq: 80	 99
				seq: 80	 89
				seq: 80	 78
				seq: 80	 3
				comb: 99	 89
				comb: 80	 80
				2	160
				1	188
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer t1, Integer t2) throws Exception {
				System.out.println("seq: " + t1 + "\t " + t2);
				return Math.max(t1, t2);
			}
		},new Function2<Integer, Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer t1, Integer t2) throws Exception {
				System.out.println("comb: " + t1 + "\t " + t2);
				return t1+t2;
			}
			
		});
		List<Tuple2<Integer,Integer>> resultRdd = aggregateByKey.collect();
		 for (Tuple2<Integer, Integer> tuple2 : resultRdd) {
			System.out.println(tuple2._1+"\t"+tuple2._2);
		}
	}
}
