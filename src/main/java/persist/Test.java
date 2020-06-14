package main.java.persist;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class Test {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("cache").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/**
		 * 1、cache是懒执行，需要有action算子来触发才会将rdd的结果持久化到内存
		 * 2、rdd持久
		 * 化完成之后必须赋值给一个变量
		 * 3、在RDD持久化后不能直接接一个action算子
		 */
		JavaRDD<String> lines = sc.textFile("spark.org");
		
		
		
		/**
		 * 1、在持久化后不能直接使用action类的算子
		 * 2、cache之后的返回值要赋值给一个rdd
		 * MEMORY_ONLY
		 * 
		 * cache也是懒执行
		 *  1、对某一个RDD执行持久化操作后要讲持久化的结果赋值到一个变量。下次直接基于这个变量来操作，操作的就是缓存中的数据。
		 *  2、持久化操作后不会立即跟action类算子。
		 * 
		 */
//		lines =  lines.cache();
		lines.persist(StorageLevel.MEMORY_ONLY());
		
		long beginTime = System.currentTimeMillis();
		long count = lines.count();
		System.out.println(count);
		long endTime = System.currentTimeMillis();
		System.out.println("cost " + (endTime - beginTime) + " milliseconds");
		
		beginTime = System.currentTimeMillis();
		count = lines.count();
		System.out.println(count);
		endTime = System.currentTimeMillis();
		System.out.println("cost " + (endTime - beginTime) + " milliseconds");
		
		sc.close();
	}
}
