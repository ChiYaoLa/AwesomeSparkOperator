package main.java.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class CollectOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ReduceOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 有一个集合，里面有1到10，10个数字，现在我们通过reduce来进行累加
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> numbers = sc.parallelize(numberList);
		
		JavaRDD<Integer> doubleNumbers = numbers.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v) throws Exception {
				return v * 2;
			}
		});
		
		// 用foreach action操作，collect在远程集群上遍历RDD的元素
		// 用collect操作，讲分布式 的在远程集群里面的数据拉取到本地！！！
		// 这种方式不建议使用，如果数据量大，走大量的网络传输
		// 甚至有可能OOM内存溢出，通常情况下你会看到用foreach操作
		List<Integer> doubleNumberList = doubleNumbers.collect();
		for(Integer num : doubleNumberList){
			System.out.println(num);
		}

		sc.close();
	}
}
