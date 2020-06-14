package main.java.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class IntersectionOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SampleOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<String> names = Arrays
				.asList("xurunyun", "liangyongqi", "wangfei","yasaka");
		List<String> names1 = Arrays
				.asList("xurunyun", "liangyongqi2", "wangfei3","yasaka4");
		JavaRDD<String> nameRDD = sc.parallelize(names,1);
		JavaRDD<String> nameRDD1 = sc.parallelize(names1,1);
		
		nameRDD.intersection(nameRDD1).foreach(new VoidFunction<String>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String name) throws Exception {
				System.out.println(name);
			}
		});
	}
}
