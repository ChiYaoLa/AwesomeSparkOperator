package main.java.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

public class MapPartitonsWithIndexOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("MapPartitonsWithIndexOperator").setMaster(
				"local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 准备一下数据
		List<String> names = Arrays
				.asList("xurunyun", "liangyongqi", "wangfei");
		// 集群模式  每台服务器 会有资源 --> CPU CORE 、 Memory
		// 一般情况下一台机器里面有一个executor进程来运行spark的task
		// 一台机器的CPU CORE核的数量就分配给一个executor来使用
		// 并行度设置为多少合适呢？理论上就是 机器的数量N 乘以 每台机器的CPU 核数
		// 如果一台机器上面跑了好几个executor进程 并行度理论上等于
		// 机器的数量N * 每个executor分配的的CPU 核数 * n个 executor
		// 实际上企业里面去设置都是这个理论值的2~3倍！！！
		JavaRDD<String> nameRDD = sc.parallelize(names,2);
		

		JavaRDD<String> mapPartitionsWithIndex = nameRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer partitionId, Iterator<String> iterator) throws Exception {
				System.out.println("partitionId:"+partitionId);
				while (iterator.hasNext()) {
					System.out.println(iterator.next());
				}
				return Arrays.asList("aaa").iterator();
			}
		}, false);
		List<String> collect = mapPartitionsWithIndex.collect();
		
		while(true){}
	}
}
