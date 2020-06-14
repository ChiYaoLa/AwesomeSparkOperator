package main.java.optimization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;import scala.Tuple4;

public class RepartitionAndSortWithinPartitionsOperator {
	public static void main(String[] args) {
		/**
		 * SparkConf是设置spark运行时的环境变量，可以设置setMaster  setAppName，设置运行时所需要的资源情况
		 */
		SparkConf conf = new SparkConf().setAppName("JoinOperator")
				.setMaster("local");
		
		//SparkContext非常的重要，SparkContext是通往集群的唯一通道，在SparkContext初始化的时候会创建任务调度器
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<Integer, String>> nameList = Arrays.asList(
				new Tuple2<Integer,String>(1, "xuruyun"),
				new Tuple2<Integer,String>(2, "liangyongqi"),
				new Tuple2<Integer,String>(5, "liangyongqi"),
				new Tuple2<Integer,String>(6, "liangyongqi"),
				new Tuple2<Integer,String>(4, "liangyongqi"),
				new Tuple2<Integer,String>(3, "wangfei"));
		
		JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(nameList);
		
		JavaPairRDD<Integer, String> repartitionAndSortWithinPartitions = nameRDD.repartitionAndSortWithinPartitions(new Partitioner() {
			
			@Override
			public int numPartitions() {
				return 2;
			}
			
			@Override
			public int getPartition(Object o1) {
				return o1.hashCode() % numPartitions();
			}
		},new DefinedComparator());
		
		
		
		
		repartitionAndSortWithinPartitions.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer,String>>, Iterator<Tuple2<Integer,String>>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<Integer, String>> call(Integer partitionId, Iterator<Tuple2<Integer, String>> iterator) throws Exception {
				List<Tuple2<Integer, String>> list = new ArrayList<>();
				while (iterator.hasNext()) {
					Tuple2<Integer, String> next = iterator.next();
					list.add(new Tuple2<Integer, String>(next._1, next._2));
					System.out.println("partitionId:"+partitionId+"\tContent:"+next);
				}
				return list.iterator();
			}
		}, true).collect();
		
		
	}
}
