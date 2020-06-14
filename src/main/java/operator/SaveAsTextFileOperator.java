package main.java.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SaveAsTextFileOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SaveAsTextFileOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 有一个集合，里面有1到10，10个数字，现在我们通过reduce来进行累加
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> numbers = sc.parallelize(numberList);
		
		JavaRDD<Integer> doubledNumbers = numbers.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v) throws Exception {
				return v * 2;
			}
		});
		
		doubledNumbers.saveAsTextFile("hdfs://spark001:9000/save.txt");
		
		sc.close();
	}
}
