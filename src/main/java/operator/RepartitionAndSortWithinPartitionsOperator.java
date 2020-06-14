package main.java.operator;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import groovy.lang.Tuple;
import scala.Tuple2;

public class RepartitionAndSortWithinPartitionsOperator {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("RepartitionAndSortWithinPartitionsOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		
		List<Tuple2<Integer,Integer>> list = Arrays.asList(
				new Tuple2<Integer,Integer>(2, 3),
				new Tuple2<Integer,Integer>(1, 2),
				new Tuple2<Integer,Integer>(6, 7),
				new Tuple2<Integer,Integer>(3, 4),
				new Tuple2<Integer,Integer>(5, 6),
				new Tuple2<Integer,Integer>(4, 5)
				);
		
		JavaPairRDD<Integer,Integer> rdd = sc.parallelizePairs(list,1);
		 
	
		JavaPairRDD<Integer, Integer>  rdd1 = rdd.repartitionAndSortWithinPartitions(new Partitioner() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			/**
			 * 设置分区数据
			 * 0 1 2
			 * @return
			 */
			@Override
			public int numPartitions() {
				return 3;
			}
			
			@Override
			public int getPartition(Object key) {
				return Integer.valueOf(key+"") % numPartitions();
			}
		},new SortObj());
		
		System.out.println("rdd1.partitions().size():" + rdd1.partitions().size());
		
		
		rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer,Integer>>, Iterator<Tuple2<Integer,Integer>>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<Integer, Integer>> call(Integer v1, Iterator<Tuple2<Integer, Integer>> v2) throws Exception {
				while(v2.hasNext()){
					System.out.println("partitionId:" + v1 + "value:" + v2.next());
				}
				return v2;
			}
		}, true).count();
		
		while(true){
			
		}
		
	}
}
