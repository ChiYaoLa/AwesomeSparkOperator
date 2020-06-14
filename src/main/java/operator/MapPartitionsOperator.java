package main.java.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

// 理解里面final使用的原因！

public class MapPartitionsOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JoinOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 准备一下数据
		List<String> names = Arrays.asList("xurunyun","liangyongqi","wangfei");
		JavaRDD<String> nameRDD = sc.parallelize(names);
		
	 
		
		// mapPartitions
		// map算子，一次就处理一个partition的一条数据！！！
		// mapPartitions算子，一次处理一个partition中所有的数据！！！
		
		// 推荐的使用场景！！！
		// 如果你的RDD的数据不是特别多，那么采用MapPartitions算子代替map算子，可以加快处理速度
		// 比如说100亿条数据 10分区，你一个partition里面就有10亿条数据，不建议使用mapPartitions，
		// 内存溢出
		
		
		nameRDD.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(Iterator<String> iterator) throws Exception {
				List<String> list = new ArrayList<>();
				
				System.out.println("创建数据库连接");
				while (iterator.hasNext()) {
					String string = iterator.next();
					System.out.println("拼接sql语句："+string);
					list.add(string);
				}
				System.out.println("批量插入到数据库中");
				return list;
			}
		}).count();
		
		
		sc.close();
	}
}
