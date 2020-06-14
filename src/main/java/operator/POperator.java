package main.java.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.Accumulator;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import com.google.common.base.Optional;

import scala.Tuple2;

// 理解里面final使用的原因！

public class POperator {

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
				new Tuple2<Integer,String>(4, "liangyongqi"),
				new Tuple2<Integer,String>(5, "liangyongqi"),
				new Tuple2<Integer,String>(6, "wangfei"));
		
		JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(nameList);
		JavaPairRDD<Integer, String> partitionBy = nameRDD.partitionBy(new Partitioner() {
			
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
		
	
		
		JavaRDD<Tuple2<Integer, Tuple2<Integer, String>>> mapPartitionsWithIndex = partitionBy.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer,String>>, Iterator<Tuple2<Integer,Tuple2<Integer,String>>>>() {

			@Override
			public Iterator<Tuple2<Integer, Tuple2<Integer, String>>> call(Integer index, Iterator<Tuple2<Integer, String>> iterator) throws Exception {
				List<Tuple2<Integer,Tuple2<Integer,String>>> list = new ArrayList<>();
				while(iterator.hasNext()){
					Tuple2<Integer, String> next = iterator.next();
					list.add(new Tuple2<Integer, Tuple2<Integer,String>>(index,next));
				}
				return list.iterator();
			}
		}, false);
		
		
		mapPartitionsWithIndex.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Integer,String>>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, Tuple2<Integer, String>> tuple2) throws Exception {
				System.out.println(tuple2);
			}
		});
		
		while(true){}
	}
}
