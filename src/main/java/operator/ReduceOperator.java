package main.java.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class ReduceOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ReduceOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 有一个集合，里面有1到10，10个数字，现在我们通过reduce来进行累加
		List<Integer> numberList = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numbers = sc.parallelize(numberList);
		
		// reduce操作的原理：首先将第一个和第二个元素，传入call方法
		// 计算一个结果，接着把结果再和后面的元素依次累加
		// 以此类推
		int sum = numbers.reduce(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		System.out.println(sum);
		sc.close();
	};

}
