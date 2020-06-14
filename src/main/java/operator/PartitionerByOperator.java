package main.java.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class PartitionerByOperator {
	 @SuppressWarnings("resource")
	public static void main(String[] args) {
		
		 
		 SparkConf conf = new SparkConf()
				 .setMaster("local")
				 .setAppName("PartitionerByOperator");
		 
		 
		 JavaSparkContext sc = new JavaSparkContext(conf);
		 
		 
		 List<Tuple2<Integer, String>> list = new ArrayList<>();
		 list.add(new Tuple2<Integer, String>(1, "bjsxt"));
		 list.add(new Tuple2<Integer, String>(2, "bjsxt"));
		 list.add(new Tuple2<Integer, String>(3, "bjsxt"));
		 list.add(new Tuple2<Integer, String>(4, "bjsxt"));
		 list.add(new Tuple2<Integer, String>(5, "bjsxt"));
		 list.add(new Tuple2<Integer, String>(6, "bjsxt"));
		 
		 JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(list);
		 System.out.println("nameRDD.partitions().size():"+nameRDD.partitions().size());
		 
		 
		 JavaPairRDD<Integer, String> partitionByRDD = nameRDD.partitionBy(new Partitioner() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public int numPartitions() {
				return 2;
			}
			
			@Override
			public int getPartition(Object obj) {
				int i = (int)obj;
				if(i % 2 == 0){
					return 0;
				}else{
					return 1;
				}
			}
		});
		 
		 partitionByRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer,String>>, Iterator<Tuple2<Integer,String>>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<Integer, String>> call(Integer v1, Iterator<Tuple2<Integer, String>> v2) throws Exception {
				 int partitionId = v1;
				while (v2.hasNext()) {
					Tuple2<Integer, String> next = v2.next();
					int id = next._1;
					String name = next._2;
					System.out.println("partitionId:"+partitionId + "= id:"+id + "|name:" + name);
					
				}
				return Arrays.asList(new Tuple2<Integer, String>(1, "asd")).iterator();
			}
		}, false).count();
		 
	}	
}
