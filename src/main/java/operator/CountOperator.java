package main.java.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CountOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ReduceOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 有一个集合，里面有1到10，10个数字，现在我们通过reduce来进行累加
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> numbers = sc.parallelize(numberList);
		
		long count = numbers.count();
		System.out.println(count);
		
		sc.close();
	}
}
