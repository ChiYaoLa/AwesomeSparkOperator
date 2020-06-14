package main.java.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

//import org.apache.hadoop.hdfs.NNBenchWithoutMR;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

/**
 * 自定义分区器
 * @author root
 *
 */
public class PartionerOperator {
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("PartionerOperator")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Tuple2<Integer, String>> list = Arrays.asList(
				new Tuple2<Integer,String>(1, "bjsxt"),
				new Tuple2<Integer,String>(2, "shsxt"),
				new Tuple2<Integer,String>(8, "sasxt"),
				new Tuple2<Integer,String>(7, "sasxt"),
				new Tuple2<Integer,String>(6, "sasxt"),
				new Tuple2<Integer,String>(5, "sasxt"),
				new Tuple2<Integer,String>(3, "sasxt"),
				new Tuple2<Integer,String>(4, "gzsxt")
				);
		
		JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(list,4);
		nameRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer,String>>, Iterator<Tuple2<Integer,String>>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<Integer, String>> call(Integer partitionId, Iterator<Tuple2<Integer, String>> dataItertor) throws Exception {
				List<Tuple2<Integer,String>> list = new ArrayList<>();
				
				
				while (dataItertor.hasNext()) {
					Tuple2<Integer, String> next = dataItertor.next();
					System.out.println("partitionId:" + partitionId + "\tdata:" + next);
					list.add(next);
				}
				
				return list.iterator();
			}
		}, false).count();
		
		
		/**
		 * map   一条一条的来处理数据
		 * mapPartrition 以partition为单位来处理，将一个partition的数据加载到内存区，然后对内存的数据进行遍历。
		 * mapPartrition 比价适合网数据库中写数据。
		 * 
		 * 
		 * mappartion有一个问题？ partition的大小是没有限制的，非常的大， 这个内存数据结构会放不下，OOM
		 *
		 */
		JavaPairRDD<Integer, String> coalesce = nameRDD.coalesce(3, false);
	
		
		/**
		 * repartition实际在内部调用的额是coalesce（numPartiion，true）
		 */
		JavaPairRDD<Integer, String> repartition = nameRDD.repartition(4);
		coalesce.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer,String>>, Iterator<Tuple2<Integer,String>>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<Integer, String>> call(Integer partitionId, Iterator<Tuple2<Integer, String>> dataItertor) throws Exception {
				List<Tuple2<Integer,String>> list = new ArrayList<>();
				
				
				while (dataItertor.hasNext()) {
					Tuple2<Integer, String> next = dataItertor.next();
					System.out.println("partitionId:" + partitionId + "\tdata:" + next);
					list.add(next);
				}
				
				return list.iterator();
			}
		}, false).count();
		
		/* nameRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer,String>>, Iterator<Tuple2<Integer,String>>>() {

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<Integer, String>> call(Integer partitionId, Iterator<Tuple2<Integer, String>> dataItertor) throws Exception {
				List<Tuple2<Integer,String>> list = new ArrayList<>();
				
				
				while (dataItertor.hasNext()) {
					Tuple2<Integer, String> next = dataItertor.next();
					System.out.println("partitionId:" + partitionId + "\tdata:" + next);
					list.add(next);
				}
				
				return list.iterator();
			}
		}, false).count();
		
		
		JavaPairRDD<Integer, String> partitionBy = nameRDD.partitionBy(new Partitioner() {
			
			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public int numPartitions() {
				return 3;
			}
			
			@Override
			public int getPartition(Object obj) {
				return obj.hashCode() % numPartitions();
			}
		});
		
		JavaRDD<Tuple2<Integer, String>> mapPartitionsWithIndex = partitionBy.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer,String>>, Iterator<Tuple2<Integer,String>>>() {

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<Integer, String>> call(Integer partitionId, Iterator<Tuple2<Integer, String>> dataItertor) throws Exception {
				List<Tuple2<Integer,String>> list = new ArrayList<>();
				
				while (dataItertor.hasNext()) {
					Tuple2<Integer, String> next = dataItertor.next();
					System.out.println("partitionId:" + partitionId + "\tdata:" + next);
					list.add(next);
				}
				
				return list.iterator();
			}
		}, false);
		
		mapPartitionsWithIndex.count();*/
	}
}
