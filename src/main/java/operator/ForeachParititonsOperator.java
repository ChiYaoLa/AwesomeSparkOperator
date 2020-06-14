package main.java.operator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class ForeachParititonsOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ForeachParititonsOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 准备一下数据
		List<String> names = Arrays.asList("xurunyun","liangyongqi","Angelababy");
		JavaRDD<String> nameRDD = sc.parallelize(names);
		
		nameRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Iterator<String> iterator) throws Exception {
				 
				System.out.println("创建数据库连接");
				while(iterator.hasNext()){
					String next = iterator.next();
					System.out.println("拼接sql语句"+next);
				}
				System.out.println("批量插入数据库");
			}
		});
		
		
	}
}
